from pyspark.sql import functions as F

filtered_stores = (
    df_stores
    .filter((F.col("region") == "South") & F.col("opened_date").between("2024-02-01", "2025-05-01"))
    .withColumn("clean_region", F.trim("region"))
)

stores_stats = (
    filtered_stores
    .groupBy("region", "city")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("region") == "North", 1).otherwise(0))) / F.count("*")), 1).alias("pct_north"),
        F.sum("sqft").alias("total_sqft"),
    )
)

result = (
    stores_stats
    .filter(F.col("total_sqft") >= 37000)
    .select("region", "city", "pct_north", "total_sqft")
    .orderBy(F.col("total_sqft").desc())
)
