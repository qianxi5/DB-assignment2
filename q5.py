import sys 
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
# you may add more import if you need to

# don't change this line
hdfs_nn = sys.argv[1]

spark = SparkSession.builder.appName("Assigment 2 Question 5").getOrCreate()
# YOUR CODE GOES BELOW
df = spark.read.option("header",True).parquet("hdfs://%s:9000/assignment2/part2/input/" % (hdfs_nn))

df.select(col('movie id'), col('title'), col('cast')).show()





