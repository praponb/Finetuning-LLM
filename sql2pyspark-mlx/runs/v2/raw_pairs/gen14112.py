from pyspark.sql import functions as F

result = (
    df_stores
    .withColumn("sqft_str", F.col("sqft").cast("double"))
    .withColumn("region_length", F.length("region"))
)
