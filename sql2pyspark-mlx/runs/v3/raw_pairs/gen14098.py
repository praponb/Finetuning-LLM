from pyspark.sql import functions as F

result = (
    df_stores
    .filter((F.upper(F.trim("store_name")) == "MALL CENTRAL") | (F.col("sqft") > 10771))
    .groupBy("city", F.quarter("opened_date").alias("opened_quarter"))
    .agg(
        F.sum(F.when(F.col("city") == "Berlin", F.col("sqft")).otherwise(0)).alias("berlin_sqft"),
        F.min("sqft").alias("min_sqft"),
    )
)
