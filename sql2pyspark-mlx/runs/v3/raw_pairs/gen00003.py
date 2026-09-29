from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("city") == "Bangkok")
    .groupBy("city", F.quarter("opened_date").alias("opened_quarter"))
    .agg(
        F.min("sqft").alias("min_sqft"),
        F.sum(F.when(F.col("city") == "New York", F.col("sqft")).otherwise(0)).alias("new_york_sqft"),
    )
    .filter(F.col("min_sqft") >= 5200)
    .orderBy("city", "opened_quarter")
    .limit(20)
)
