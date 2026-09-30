from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("region").isin(["South", "Central", "North"]) & ((F.col("city") == "Singapore") | (F.col("opened_date") < "2024-03-01")))
    .groupBy("region")
    .agg(
        F.sum(F.when(F.col("region") == "East", F.col("sqft")).otherwise(0)).alias("east_sqft"),
        F.count(F.when(F.col("city") == "New York", 1)).alias("new_york_count"),
        F.count("*").alias("store_count"),
    )
    .filter(F.col("store_count") >= 5)
    .drop("store_count")
)
