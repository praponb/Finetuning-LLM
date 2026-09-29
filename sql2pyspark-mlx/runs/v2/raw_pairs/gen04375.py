from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy("city", F.quarter("opened_date").alias("opened_quarter"), "region")
    .agg(
        F.avg("sqft").alias("avg_sqft"),
        F.sum(F.when(F.col("city") == "New York", F.col("sqft")).otherwise(0)).alias("new_york_sqft"),
    )
    .filter(F.col("new_york_sqft") < 24)
)
