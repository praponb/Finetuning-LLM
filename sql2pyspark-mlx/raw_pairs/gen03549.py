from pyspark.sql import functions as F

result = (
    df_departments
    .withColumn("location_prefix", F.substring("location", 1, 4))
    .withColumn("location_length", F.length("location"))
)
