# -*- coding: utf-8 -*-
"""
Production Aggregation Workflows
Created on Thu Jun 30 16:23:53 2022

@author: cskinner
"""

from asyncio.windows_events import NULL
import datetime
from logging import root
from sqlite3 import Timestamp
from threading import local
from tkinter import E
from tracemalloc import start
import arcpy, os

#run parameters
resetAll_ = False            # deletes the output geodatabase
reload_from_snowflake = False #reloads geodatabase from snowflake data source
prep_well_data = False  # prepares well header columns.  Testing 8/24/2022
run_well_aggregate = False   #runs the well aggregation procedures
run_well_aggregate_5_10 = True # also runs 5 and 10 year well aggregates
rename_wells = False         #runs the renaming of well columns
run_prod_aggregate = False  #runs the production aggregation procesure
run_prod_aggregate_5_10 = True # runs the 5 and 10 year production aggregates
rename_prod = False         #runs the renaming of production columns
rename_prod2 = False  # delete this soon
remove_nulls = False       #cleans up null records in the production outputs
create_centroids = False    # creates point centroids for all layers
arcpy.env.overwriteOutput = True

#TODO : architect better dates for data processing, key date is when the data is grabbed from snowflake!
version_key = '_Sep_1_2022'
run_date = datetime.date.today()   


rootFilePath_ = r'D:\cskinner\OneDrive - DOI\ArcGIS\Projects\USOilGasWellAggregation'
local_geodatabase_name_ = "USOilGasDataInput" + version_key + ".gdb"
local_geodatabase_ = os.path.join(rootFilePath_, local_geodatabase_name_)
snowflake_map_ = r'D:\cskinner\OneDrive - DOI\Documents\ArcGIS\Projects\IHSOilGasSnowflakeDataSets\IHSOilGasSnowflakeDataSets.aprx'
output_geodatabase_name_ =  r'Aggregate' + version_key + '.gdb'
output_geodatabase_ = os.path.join(rootFilePath_, output_geodatabase_name_)

if resetAll_ or not arcpy.Exists( local_geodatabase_):
    arcpy.CreateFileGDB_management(rootFilePath_,local_geodatabase_name_)

if resetAll_ or not arcpy.Exists(output_geodatabase_):
    arcpy.CreateFileGDB_management(rootFilePath_,output_geodatabase_name_)


#PRojection Definition -- this is used for all aspects of this analysis, from data import through final aggregations
outputCoordinateSystem_ = arcpy.SpatialReference("North_America_Albers_Equal_Area_Conic")
arcpy.env.scratchWorkspace  = r"C:\Temp\Temp.gdb"
arcpy.env.cartographicCoordinateSystem = outputCoordinateSystem_
arcpy.EnvManager.workspace = output_geodatabase_name_

#define variable for local well header
Well_Header_Local_ = local_geodatabase_ + r'\Well_Header_Local_Albers'
Well_Header_Local_lyr_ = os.path.join(rootFilePath_, 'Well_Header_Local' + version_key + '.lyrx')

#Production variables
production_abstract_local_ = local_geodatabase_ + r'\US_Production_Abstract_Local_Albers'
production_annual_local_ = local_geodatabase_ + r'\US_Production_Annual_Local_Albers'
production_local_lyr_ = os.path.join(rootFilePath_, 'Production_Local' + version_key + '.lyrx')

#treatment data
treated_wells_local_ = local_geodatabase_ + r'\Fractured_Wells_Local'


#Well Aggregate Outputs
well_aggregate_1_1_ = output_geodatabase_ +  r'\USWells_1Mile_1Year' 
well_aggregate_10_1_ = output_geodatabase_ +  r'\USWells_10Mile_1Year' 
well_aggregate_1_ = output_geodatabase_ +  r'\USWells_1Mile' 
well_aggregate_10_ = output_geodatabase_ +  r'\USWells_10Mile' 
well_aggregate_10_5_ = output_geodatabase_ +  r'\USWells_10Mile_5Year' 
well_aggregate_10_10_ = output_geodatabase_ +  r'\USWells_10Mile_10Year' 

#Production Aggregate Outputs
production_aggregate_2_ = output_geodatabase_ + r'\USProduction_2Mile' 
production_aggregate_2_1_ = output_geodatabase_ + r'\USProduction_2Mile_1Year' 
production_aggregate_10_ = output_geodatabase_ + r'\USProduction_10Mile' 
production_aggregate_10_1_ = output_geodatabase_ + r'\USProduction_10Mile_1Year' 
production_aggregate_10_5_ = output_geodatabase_ + r'\USProduction_10Mile_5Year' 
production_aggregate_10_10_ = output_geodatabase_ + r'\USProduction_10Mile_10Year' 

#copies well and production data into local systems for processing
#Copy features from snowflake into local version
#this will perform a projection into Output Coordinate system (Albers equal area)
def cacheData():
    
    sde_file_ = r'D:\cskinner\OneDrive - DOI\Documents\ArcGIS\Projects\IHSOilGasSnowflakeDataSets\IHSSnowflake.sde'

    #Fractured Wells from Snowflake to local version
    qry_ = r"select distinct UWI, GEOGRAPHY from WELL.WELL_TREATMENT_SUMMARY WHERE TREATMENT_TYPE LIKE '%FRACTURE%' AND COUNTRY = 'UNITED STATES OF AMERICA'"
    print('Pulling Fractured Wells Layer: {}'.format(qry_))
    with arcpy.EnvManager(outputCoordinateSystem=outputCoordinateSystem_, workspace=local_geodatabase_):
        arcpy.MakeQueryLayer_management(input_database = sde_file_,out_layer_name = 'QueryLayer', query = qry_, oid_fields= ('UWI'), shape_type = 'POINT', spatial_properties='DO_NOT_DEFINE_SPATIAL_PROPERTIES')
        arcpy.management.CopyFeatures(in_features='QueryLayer', out_feature_class=treated_wells_local_, config_keyword="", spatial_grid_1=None, spatial_grid_2=None, spatial_grid_3=None)


    #Header from Snowflake query to local version
    qry_ = r"select UWI, CURRENT_STATUS, CURRENT_STATUS_GROUP, FIRST_SPUD_DATE,SPUD_DATE, GEOGRAPHY_SURFACE from WELL.WELL_HEADER WHERE COUNTRY = 'UNITED STATES OF AMERICA' AND (CURRENT_STATUS_GROUP  LIKE '%OIL%' OR CURRENT_STATUS_GROUP LIKE '%GAS%' OR CURRENT_STATUS_GROUP LIKE '%DRY%' OR CURRENT_STATUS_GROUP  LIKE '%SUSPENDED - PRODUCER%' OR CURRENT_STATUS_GROUP LIKE '%INJECTION%' ) AND CURRENT_STATUS NOT LIKE '%GEOTHERMAL%'"
    print('Pulling Well Header Layer {} time: {}'.format(qry_, datetime.datetime.now()))
    with arcpy.EnvManager(outputCoordinateSystem=outputCoordinateSystem_, workspace=local_geodatabase_):
        arcpy.MakeQueryLayer_management(input_database = sde_file_, out_layer_name = 'HeaderLayer', query = qry_, oid_fields= 'UWI', shape_type = 'POINT', spatial_properties='DO_NOT_DEFINE_SPATIAL_PROPERTIES')
        arcpy.management.CopyFeatures(in_features='HeaderLayer', out_feature_class=Well_Header_Local_, config_keyword="", spatial_grid_1=None, spatial_grid_2=None, spatial_grid_3=None)

    #production Abstract layer to local version
    qry_ = r"select CUM_COMBINED_GAS_VOL_USCUST, CUM_LIQUID_VOL_USCUST, CUM_WATER_VOL_USCUST, ENTITY, ENTITY_TYPE, GEOGRAPHY from PRODUCTION.PRODUCTION_ABSTRACT WHERE country = 'UNITED STATES OF AMERICA' and (entity_type = 'LEASE' or entity_type = 'WELL')"
    print('Pulling Production (Abstract) Layer: {}'.format(qry_))
    with arcpy.EnvManager(outputCoordinateSystem=outputCoordinateSystem_, workspace=local_geodatabase_):
        arcpy.MakeQueryLayer_management(input_database = sde_file_,out_layer_name = 'ProdAbstract', query = qry_, oid_fields= ('ENTITY;ENTITY_TYPE'), shape_type = 'POINT', spatial_properties='DO_NOT_DEFINE_SPATIAL_PROPERTIES')
        arcpy.management.CopyFeatures(in_features='ProdAbstract', out_feature_class=production_abstract_local_, config_keyword="", spatial_grid_1=None, spatial_grid_2=None, spatial_grid_3=None)

    #production annual layer to local version
    qry_ = r"select ENTITY, ENTITY_TYPE, YEAR, COMBINED_GAS_VOL_USCUST, LIQUID_VOL_USCUST, WATER_VOL_USCUST, GEOGRAPHY from PRODUCTION.PRODUCTION_ANNUAL WHERE country = 'UNITED STATES OF AMERICA' and (entity_type = 'LEASE' or entity_type = 'WELL') UNION SELECT A.Entity ENTITY, ENTITY_TYPE, YEAR - 1, A.PRIOR_CUM_COMBINED_GAS_VOL_USCUST COMBINED_GAS_VOL_USCUST, PRIOR_CUM_LIQUID_VOL_USCUST LIQUID_VOL_USCUST, PRIOR_CUM_WATER_VOL_USCUST WATER_VOL_USCUST, GEOGRAPHY FROM PRODUCTION.PRODUCTION_ANNUAL A,(select ENTITY, min(year) FIRST_YEAR  from PRODUCTION.PRODUCTION_ANNUAL where country = 'UNITED STATES OF AMERICA' and (entity_type = 'LEASE' or entity_type = 'WELL') GROUP BY Entity ) B WHERE A.ENTITY = B.ENTITY AND A.YEAR = FIRST_YEAR AND country = 'UNITED STATES OF AMERICA'  and (entity_type = 'LEASE' or entity_type = 'WELL') AND (PRIOR_CUM_COMBINED_GAS_VOL_USCUST IS NOT NULL or PRIOR_CUM_LIQUID_VOL_USCUST IS NOT NULL or PRIOR_CUM_WATER_VOL_USCUST IS NOT null)"
    print('Pulling Production (Annual) Layer: {}'.format(qry_))
    with arcpy.EnvManager(outputCoordinateSystem=outputCoordinateSystem_, workspace=local_geodatabase_):
        arcpy.MakeQueryLayer_management(input_database = sde_file_,out_layer_name = 'AnnualLayer', query = qry_, oid_fields= ('ENTITY;ENTITY_TYPE;YEAR'), shape_type = 'POINT', spatial_properties='DO_NOT_DEFINE_SPATIAL_PROPERTIES')
        arcpy.management.CopyFeatures(in_features='AnnualLayer', out_feature_class=production_annual_local_, config_keyword="", spatial_grid_1=None, spatial_grid_2=None, spatial_grid_3=None)



   
#adds indexes, calculates fields for local data sets. Also joins FRACTURED_WELL from treatment table. 
def prepWellData():
   
    try:
        arcpy.management.AddIndex(Well_Header_Local_, "UWI", "UWI_idx", "UNIQUE", "NON_ASCENDING")
        
        arcpy.management.AddIndex(Well_Header_Local_, "CURRENT_STATUS_GROUP", "CSG_idx", "NON_UNIQUE", "NON_ASCENDING")
        arcpy.management.AddIndex(Well_Header_Local_, "HOLE_DIRECTION", "HOLE_DIR_idx", "NON_UNIQUE", "NON_ASCENDING")  
    except:
        print('errors in index creation')
    try:    
        # create index for UWI in treated wells, which will improve join
        arcpy.management.AddIndex(treated_wells_local_, "UWI", "UWIidx", "UNIQUE", "NON_ASCENDING")
    except:
        print('errors in index creation')
    
    #when spud_date is null, use first_spud date.  There were appx 1500 wells with null spuds as of 5/2/2022. These are typically 'other' wells, such as 'well start', 'permitted'
    arcpy.management.CalculateField(Well_Header_Local_, "SPUD_DATE", "conDate(!SPUD_DATE!, !FIRST_SPUD_DATE!)", "PYTHON3", """def conDate(spud_d, fspud_d):
        if spud_d is None:
            return fspud_d;
        else:
            return spud_d;""", "TEXT", "NO_ENFORCE_DOMAINS")

    # Create new fields (to be calculated in next step)
    arcpy.management.AddFields(Well_Header_Local_, "OIL_WELL SHORT # # # #;GAS_WELL SHORT # # # #;DRY_WELL SHORT # # # #;INJECTION_WELL SHORT # # # #;HORIZONTAL_WELL SHORT # # # #")
    
    #The update cursor goes through well header 1 row at a time (yuck!) calculating the flags for well types.  This was done since it compares current_status_group and Current_status columns.
    cursor = arcpy.UpdateCursor(Well_Header_Local_)
    for row in cursor:

        csg_ = row.getValue("CURRENT_STATUS_GROUP")
        if 'OIL' in csg_:
            row.setValue("OIL_WELL", 1)
        if 'GAS' in csg_:
            row.setValue("GAS_WELL", 1)
        if 'DRY' in csg_:
            row.setValue("DRY_WELL", 1)
        if 'INJECTION' in csg_:
            row.setValue("INJECTION_WELL", 1)
        if 'SUSPENDED' in csg_:
            if 'OIL' in row.getValue("CURRENT_STATUS"):
                row.setValue("OIL_WELL", 1)
            if 'GAS' in row.getValue("CURRENT_STATUS"):
                row.setValue("GAS_WELL", 1)
        if row.getValue("HOLE_DIRECTION") != None and 'HORIZONTAL' in row.getValue("HOLE_DIRECTION"):
            row.setValue("HORIZONTAL_WELL", 1)
        cursor.updateRow(row)
    del cursor, row
    

    # Process: Calculate Field (FRACTURED_WELLS) (Calculate Field) (management)
    #this creates and populated the field 'FRACTURED_WELL' = 1 for all records in the treated_wells_local table
    arcpy.management.CalculateField(in_table=treated_wells_local_, field="FRACTURED_WELL", expression="1", expression_type="ARCADE", code_block="", field_type="TEXT", enforce_domains="NO_ENFORCE_DOMAINS")
    ## Join matching treated_wells_local table to Well Header.  The 'FRACTURED_WELL' tables is inserted where they match.
    arcpy.management.JoinField(Well_Header_Local_, "UWI", treated_wells_local_, "UWI", "FRACTURED_WELL")
    
     
    

#this sets up layer files (.lyrx), which are needed to represent a time-registered layer for aggregation. Datasets natively are not time-registered.
def setLayerFiles():
    #well header layer file validation
    if  os.path.exists(Well_Header_Local_lyr_) == False:
        layerName_ = 'Well Local Spud Date'
        print('Creating layer file')
        with(arcpy.EnvManager(scratchWorkspace=output_geodatabase_, outputCoordinateSystem=outputCoordinateSystem_, workspace=local_geodatabase_)):
            arcpy.MakeFeatureLayer_management(in_features=Well_Header_Local_, out_layer = layerName_, where_clause='')
            arcpy.SaveToLayerFile_management(layerName_, Well_Header_Local_lyr_)
    lyrFile = arcpy.mp.LayerFile(Well_Header_Local_lyr_)
    for lyr in lyrFile.listLayers():
        if lyr.supports('TIME'):
            if lyr.isTimeEnabled == False:
                print('Setting up time-enabled layer for well header based on SPUD_DATE')
                lyr.startTimeField = 'SPUD_DATE'
                lyr.enableTime('SPUD_DATE', None, False)
                lyrFile.save()
        else:
            print("Time is not supported on this layer")
    

    #production
    if  os.path.exists(production_local_lyr_) == False:
        layerName_ = 'Production Annual'
        with(arcpy.EnvManager(scratchWorkspace=output_geodatabase_, outputCoordinateSystem=outputCoordinateSystem_, workspace=local_geodatabase_)):
            arcpy.MakeFeatureLayer_management(in_features=production_annual_local_, out_layer = layerName_, where_clause="ENTITY_TYPE = 'LEASE' or ENTITY_TYPE = 'WELL' or ENTITY_TYPE = 'FIRST_YEAR'")
            arcpy.SaveToLayerFile_management(layerName_, production_local_lyr_)
    lyrFile = arcpy.mp.LayerFile(production_local_lyr_)
    for lyr in lyrFile.listLayers():
        if lyr.supports('TIME'):
            if lyr.isTimeEnabled == False:
                print('Setting up time-enabled layer for production layer (year)')
                lyr.startTimeField = 'YEAR'
                lyr.timeFormat = 'YYYY'
                lyr.enableTime('YEAR', None, False)
                lyrFile.save()
        else:
            print("Time is not supported on this layer")
        
##this is not used in this script, was created to more generically create time-registered layer files.
def getTimeRegisteredLayer(InputLayer_, startTime_,  endTime_, overWriteIfExists, whereClause_):
    lyrName = os.path.split(InputLayer_)[1]
    lyrFileLocation = os.path.join(rootFilePath_, lyrName + '.lyrx')
    

    if  os.path.exists(lyrFileLocation) == False or overWriteIfExists == True:
        print('Creating .lyrx file: ' + lyrFileLocation)
        layerName_ = lyrName
        arcpy.MakeFeatureLayer_management(in_features=InputLayer_, out_layer = layerName_,where_clause=whereClause_)
        arcpy.SaveToLayerFile_management(layerName_, lyrFileLocation)
    lyrFile = arcpy.mp.LayerFile(lyrFileLocation)
    for lyr in lyrFile.listLayers():
        if lyr.supports('TIME'):
            if lyr.isTimeEnabled == False:
                print('Now setting up time-enabled layer...')
                lyr.startTimeField = startTime_
                lyr.endTimeField = endTime_
                lyr.enableTime(startTime_, endTime_, True)
                lyrFile.save()
                
            else:
                print('layer file is already time-registered')
        else:
            print("Time is not supported on this layer")
    return lyrFileLocation



def AggregateLayer(InputLayer, OutputLayer, Size, Time, SummaryFields):
    
    with arcpy.EnvManager(scratchWorkspace=output_geodatabase_, outputCoordinateSystem=outputCoordinateSystem_, workspace=local_geodatabase_):
        print(f"AggregatePoints: {InputLayer}, {OutputLayer}, {Size}, {Time}, {SummaryFields}")
        arcpy.gapro.AggregatePoints(InputLayer,OutputLayer, "BIN", None, "SQUARE", Size, Time, None, None, SummaryFields)
     
if reload_from_snowflake:
    cacheData()

if prep_well_data:
    prepWellData()

#sets up layer files for aggregate points.
setLayerFiles()

#Well aggregation processing
if run_well_aggregate:
    AggregateLayer(Well_Header_Local_lyr_, well_aggregate_10_, '10 Miles', '' , "OIL_WELL COUNT;GAS_WELL COUNT;DRY_WELL COUNT;INJECTION_WELL COUNT;HORIZONTAL_WELL COUNT;FRACTURED_WELL COUNT")
    AggregateLayer(Well_Header_Local_lyr_, well_aggregate_1_, '1 Miles', '' , "OIL_WELL COUNT;GAS_WELL COUNT;DRY_WELL COUNT;INJECTION_WELL COUNT;HORIZONTAL_WELL COUNT;FRACTURED_WELL COUNT")

    AggregateLayer(Well_Header_Local_lyr_, well_aggregate_10_1_, '10 Miles', '1 Year' , "OIL_WELL COUNT;GAS_WELL COUNT;DRY_WELL COUNT;INJECTION_WELL COUNT;HORIZONTAL_WELL COUNT;FRACTURED_WELL COUNT")
    AggregateLayer(Well_Header_Local_lyr_, well_aggregate_1_1_, '1 Miles', '1 Year' , "OIL_WELL COUNT;GAS_WELL COUNT;DRY_WELL COUNT;INJECTION_WELL COUNT;HORIZONTAL_WELL COUNT;FRACTURED_WELL COUNT")

#Well aggregation field renaming for consistency
if rename_wells:
    for file_ in [well_aggregate_10_1_, well_aggregate_1_1_]:
        print(f"Altering fields for {file_}...")
        arcpy.management.AlterField(file_, "COUNT", "COUNT_WELL", '')
        arcpy.management.CalculateField(file_, "YEAR", "YEAR($feature.START_DATE)", "ARCADE", '', "SHORT", "NO_ENFORCE_DOMAINS")
        arcpy.management.DeleteField(file_, "START_DATE;END_DATE", "DELETE_FIELDS")
    for file_ in [well_aggregate_10_, well_aggregate_1_]:
        print(f"Altering fields for {file_}...")
        arcpy.management.AlterField(file_, "COUNT", "COUNT_WELL", '')

if run_well_aggregate_5_10:
    print('running 10 mile, 5 and 10 year well aggregations...')
    AggregateLayer(Well_Header_Local_lyr_, well_aggregate_10_5_, '10 Miles', '5 Year' , "OIL_WELL COUNT;GAS_WELL COUNT;DRY_WELL COUNT;INJECTION_WELL COUNT;HORIZONTAL_WELL COUNT;FRACTURED_WELL COUNT")
    AggregateLayer(Well_Header_Local_lyr_, well_aggregate_10_10_, '10 Miles', '10 Year' , "OIL_WELL COUNT;GAS_WELL COUNT;DRY_WELL COUNT;INJECTION_WELL COUNT;HORIZONTAL_WELL COUNT;FRACTURED_WELL COUNT")
    for file_ in [well_aggregate_10_5_, well_aggregate_10_10_]:
        print(f"Altering fields for {file_}...")
        arcpy.management.AlterField(file_, "COUNT", "COUNT_WELL", '')
        arcpy.management.CalculateField(file_, "YEAR", "YEAR($feature.START_DATE)", "ARCADE", '', "SHORT", "NO_ENFORCE_DOMAINS")
        arcpy.management.DeleteField(file_, "START_DATE;END_DATE", "DELETE_FIELDS")

#production aggregation processing
if run_prod_aggregate:
    # 2 Mile Production processing
    #Production aggregation processing (2 mile, 1 year increments)
    AggregateLayer(production_local_lyr_, production_aggregate_2_1_, '2 Miles', '1 Year' , "COMBINED_GAS_VOL_USCUST SUM;LIQUID_VOL_USCUST SUM;WATER_VOL_USCUST SUM")
    # 10 Mile Production processing ( 1 year increment)
    AggregateLayer(production_local_lyr_, production_aggregate_10_1_, '10 Miles', '1 Year' , "COMBINED_GAS_VOL_USCUST SUM;LIQUID_VOL_USCUST SUM;WATER_VOL_USCUST SUM")
    
    #Production aggregation processing (2 mile no year)
    AggregateLayer(production_abstract_local_, production_aggregate_2_, '2 Miles', '' , "CUM_COMBINED_GAS_VOL_USCUST SUM;CUM_LIQUID_VOL_USCUST SUM;CUM_WATER_VOL_USCUST SUM")
    #Production aggregation processing (10 mile no year)
    AggregateLayer(production_abstract_local_, production_aggregate_10_, '10 Miles', '' , "CUM_COMBINED_GAS_VOL_USCUST SUM;CUM_LIQUID_VOL_USCUST SUM;CUM_WATER_VOL_USCUST SUM")


#renames fields of production aggregations for consistency
if rename_prod:
    for file_ in [production_aggregate_10_1_, production_aggregate_2_1_]:
        print(f"Altering fields for {file_}...")
        arcpy.management.AlterField(file_, "SUM_COMBINED_GAS_VOL_USCUST", "SUM_GAS_VOL", '', "DOUBLE", 8, "NULLABLE", "CLEAR_ALIAS")
        arcpy.management.AlterField(file_, "SUM_LIQUID_VOL_USCUST", "SUM_OIL_VOL", '', "DOUBLE", 8, "NULLABLE", "CLEAR_ALIAS")
        arcpy.management.AlterField(file_, "SUM_WATER_VOL_USCUST", "SUM_WATER_VOL", '', "DOUBLE", 8, "NULLABLE", "CLEAR_ALIAS")
        arcpy.management.CalculateField(file_, "YEAR", "YEAR($feature.START_DATE)", "ARCADE", '', "SHORT", "NO_ENFORCE_DOMAINS")
        arcpy.management.DeleteField(file_, "COUNT;START_DATE;END_DATE", "DELETE_FIELDS")

    for file_ in [production_aggregate_10_, production_aggregate_2_]:
        print(f"Altering fields for {file_}...")
        arcpy.management.AlterField(file_, "SUM_CUM_COMBINED_GAS_VOL_USCUST", "SUM_GAS_VOL", '', "DOUBLE", 8, "NULLABLE", "CLEAR_ALIAS")
        arcpy.management.AlterField(file_, "SUM_CUM_LIQUID_VOL_USCUST", "SUM_OIL_VOL", '', "DOUBLE", 8, "NULLABLE", "CLEAR_ALIAS")
        arcpy.management.AlterField(file_, "SUM_CUM_WATER_VOL_USCUST", "SUM_WATER_VOL", '', "DOUBLE", 8, "NULLABLE", "CLEAR_ALIAS")
        arcpy.management.DeleteField(file_, "COUNT", "DELETE_FIELDS")


if run_prod_aggregate_5_10:
    AggregateLayer(production_local_lyr_, production_aggregate_10_10_, '10 Miles', '10 Year' , "COMBINED_GAS_VOL_USCUST SUM;LIQUID_VOL_USCUST SUM;WATER_VOL_USCUST SUM")
    # 10 Mile Production processing ( 5 year increment)
    AggregateLayer(production_local_lyr_, production_aggregate_10_5_, '10 Miles', '5 Year' , "COMBINED_GAS_VOL_USCUST SUM;LIQUID_VOL_USCUST SUM;WATER_VOL_USCUST SUM")
    for file_ in [production_aggregate_10_5_, production_aggregate_10_10_]:
        print(f"Altering fields for {file_}...")
        arcpy.management.AlterField(file_, "SUM_COMBINED_GAS_VOL_USCUST", "SUM_GAS_VOL", '', "DOUBLE", 8, "NULLABLE", "CLEAR_ALIAS")
        arcpy.management.AlterField(file_, "SUM_LIQUID_VOL_USCUST", "SUM_OIL_VOL", '', "DOUBLE", 8, "NULLABLE", "CLEAR_ALIAS")
        arcpy.management.AlterField(file_, "SUM_WATER_VOL_USCUST", "SUM_WATER_VOL", '', "DOUBLE", 8, "NULLABLE", "CLEAR_ALIAS")
        arcpy.management.CalculateField(file_, "YEAR", "YEAR($feature.START_DATE)", "ARCADE", '', "SHORT", "NO_ENFORCE_DOMAINS")
        arcpy.management.DeleteField(file_, "COUNT;START_DATE;END_DATE", "DELETE_FIELDS")
    



#this removes empty data values from the production aggregation step.
if remove_nulls:
    for file_ in [production_aggregate_10_, production_aggregate_10_1_, production_aggregate_2_1_, production_aggregate_2_]:
        print(f'removing nulls from {file_}')
        tempLayer = 'productionLayer'
        arcpy.MakeFeatureLayer_management(file_, tempLayer)
        expression = "(" + arcpy.AddFieldDelimiters(file_,"SUM_GAS_VOL") + " = 0 or " + arcpy.AddFieldDelimiters(file_,"SUM_GAS_VOL") + " is Null) and (" + arcpy.AddFieldDelimiters(file_,"SUM_OIL_VOL") + " = 0 or " + arcpy.AddFieldDelimiters(file_,"SUM_OIL_VOL") + " is Null) and (" + arcpy.AddFieldDelimiters(file_,"SUM_WATER_VOL") + " = 0 or " + arcpy.AddFieldDelimiters(file_,"SUM_WATER_VOL") + " is Null)"
        arcpy.SelectLayerByAttribute_management(tempLayer, "NEW_SELECTION", expression)
        if int(arcpy.GetCount_management(tempLayer)[0]) > 0:
            arcpy.DeleteFeatures_management(tempLayer)

# creates centroids of the output
if create_centroids:
    for file_ in [production_aggregate_10_1_, production_aggregate_2_1_, production_aggregate_10_, production_aggregate_2_, well_aggregate_10_, well_aggregate_10_1_, well_aggregate_1_, well_aggregate_1_1_]:
        with arcpy.EnvManager(outputCoordinateSystem=outputCoordinateSystem_):
            arcpy.FeatureToPoint_management(file_,file_ + '_point',point_location=None)
    # creates centroids of the output (web mercator)
    for file_ in [production_aggregate_10_1_, production_aggregate_2_1_, production_aggregate_10_, production_aggregate_2_, well_aggregate_10_, well_aggregate_10_1_, well_aggregate_1_, well_aggregate_1_1_]:
        with arcpy.EnvManager(outputCoordinateSystem=arcpy.SpatialReference("WGS_1984_Web_Mercator_Auxiliary_Sphere")):
            arcpy.FeatureToPoint_management(file_,file_ + '_pointWM',point_location=None)       


