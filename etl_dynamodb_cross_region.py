import sys
import boto3
from awsglue.transforms import *
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job

sc = SparkContext.getOrCreate()
gc = GlueContext(sc)
job = Job(gc)

#create dynamodb resource client to omit data type descriptors being returned with collections
ddb = boto3.resource('dynamodb', region_name='eu-west-1')

#get table resource in another AWS region
test_table = ddb.Table('test-table')

# get table items from scan, pull items list from dictionary
table_items = test_table.scan()['Items']

# convert to RDD so we can use it in spark
ddbRDD =  sc.parallelize(table_items)

# if you only want a DataFrame, create DataFrame from RDD using SQLContext (GlueContext can only create DynamicFrames from RDDs, not DataFrames)
#ddbDataFrame = gc.createDataFrame(ddbRDD)

#create DynamicFrame from RDD using GlueContext
ddbDynamicFrame = gc.create_dynamic_frame_from_rdd(ddbRDD, "ddbDFrame")

#write to S3
datasink = gc.write_dynamic_frame.from_options(frame = ddbDynamicFrame, connection_type = "s3",connection_options = { "path":  "s3://your-glue-target-bucket"}, format="json")

job.commit()   

