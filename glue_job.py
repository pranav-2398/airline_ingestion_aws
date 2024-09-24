import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
import re

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node airports_dim
airports_dim_node1726913464727 = glueContext.create_dynamic_frame.from_catalog(database="airline-datamart", table_name="redshift_dev_airlines_airports_dim", redshift_tmp_dir="s3://redshift-temp-bucket1/airline-data/dim/",additional_options={"aws_iam_role": "arn:aws:iam::930446798529:role/RedshiftRole"}, transformation_ctx="airports_dim_node1726913464727")

# Script generated for node raw_daily_flights_data
raw_daily_flights_data_node1726912802457 = glueContext.create_dynamic_frame.from_catalog(database="airline-datamart", table_name="raw_daily_flights", transformation_ctx="raw_daily_flights_data_node1726912802457")

# Script generated for node Filter for Departure Delay more than 60 mins
FilterforDepartureDelaymorethan60mins_node1726912925629 = Filter.apply(frame=raw_daily_flights_data_node1726912802457, f=lambda row: (row["depdelay"] >= 60), transformation_ctx="FilterforDepartureDelaymorethan60mins_node1726912925629")

# Script generated for node Join_for_dept
FilterforDepartureDelaymorethan60mins_node1726912925629DF = FilterforDepartureDelaymorethan60mins_node1726912925629.toDF()
airports_dim_node1726913464727DF = airports_dim_node1726913464727.toDF()
Join_for_dept_node1726913541351 = DynamicFrame.fromDF(FilterforDepartureDelaymorethan60mins_node1726912925629DF.join(airports_dim_node1726913464727DF, (FilterforDepartureDelaymorethan60mins_node1726912925629DF['originairportid'] == airports_dim_node1726913464727DF['airport_id']), "left"), glueContext, "Join_for_dept_node1726913541351")

# Script generated for node Modify_dep_columns
Modify_dep_columns_node1726913788151 = ApplyMapping.apply(frame=Join_for_dept_node1726913541351, mappings=[("depdelay", "long", "dep_delay", "bigint"), ("arrdelay", "long", "arr_delay", "bigint"), ("destairportid", "long", "destairportid", "long"), ("carrier", "string", "carrier", "string"), ("city", "string", "dep_city", "string"), ("name", "string", "dep_airport", "string"), ("state", "string", "dep_state", "string")], transformation_ctx="Modify_dep_columns_node1726913788151")

# Script generated for node Join_for_dest
Modify_dep_columns_node1726913788151DF = Modify_dep_columns_node1726913788151.toDF()
airports_dim_node1726913464727DF = airports_dim_node1726913464727.toDF()
Join_for_dest_node1726914283519 = DynamicFrame.fromDF(Modify_dep_columns_node1726913788151DF.join(airports_dim_node1726913464727DF, (Modify_dep_columns_node1726913788151DF['destairportid'] == airports_dim_node1726913464727DF['airport_id']), "left"), glueContext, "Join_for_dest_node1726914283519")

# Script generated for node modify_dest_columns
modify_dest_columns_node1726914321129 = ApplyMapping.apply(frame=Join_for_dest_node1726914283519, mappings=[("carrier", "string", "carrier", "string"), ("dep_state", "string", "dep_state", "string"), ("state", "string", "arr_state", "string"), ("arr_delay", "bigint", "arr_delay", "long"), ("city", "string", "arr_city", "string"), ("name", "string", "arr_airport", "string"), ("dep_city", "string", "dep_city", "string"), ("dep_delay", "bigint", "dep_delay", "long"), ("dep_airport", "string", "dep_airport", "string")], transformation_ctx="modify_dest_columns_node1726914321129")

# Script generated for node redshift_target_fact
redshift_target_fact_node1726914522143 = glueContext.write_dynamic_frame.from_catalog(frame=modify_dest_columns_node1726914321129, database="airline-datamart", table_name="redshift_dev_airlines_daily_flights_fact", redshift_tmp_dir="s3://redshift-temp-bucket1/airline-data/fact/",additional_options={"aws_iam_role": "arn:aws:iam::930446798529:role/RedshiftRole"}, transformation_ctx="redshift_target_fact_node1726914522143")

job.commit()