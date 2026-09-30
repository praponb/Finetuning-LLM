from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("sqft") == 13850)
    .groupBy("region")
    .agg(
        F.countDistinct("sqft").alias("unique_citys"),
        F.sum(F.when(F.col("region") == "West", F.col("sqft")).otherwise(0)).alias("west_sqft"),
    )
    .filter(F.col("west_sqft") < 38)
)
