from pyspark.sql import functions as F

base = (
    df_stores
    .filter(F.col("store_name").like("%Riverside%") & (F.col("opened_date") >= "2024-01-01"))
)

city_summary = (
    base
    .groupBy("city")
    .agg(
        F.count(F.when(F.col("region") == "West", 1)).alias("west_count"),
        F.count("*").alias("store_count"),
    )
)

result = (
    city_summary
    .select("city", "west_count", "store_count")
    .orderBy("store_count", F.col("west_count").desc(), "city")
    .limit(1)
)
