from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("sqft").between(6323, 11232))
    .groupBy(F.quarter("opened_date").alias("opened_quarter"), "city")
    .agg(
        F.round(F.stddev("sqft"), 2).alias("stddev_sqft"),
        F.sum(F.when(F.col("region") == "West", F.col("sqft")).otherwise(0)).alias("west_sqft"),
    )
)
