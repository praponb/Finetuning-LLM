from pyspark.sql import functions as F

recent_stores = df_stores.filter((F.col("opened_date") <= "2025-05-01") | (F.col("sqft") != 10936))

agg = (
    recent_stores
    .groupBy(
        F.when(F.col("sqft") >= 16766, "high").when(F.col("sqft") >= 2611, "mid").otherwise("low").alias("sqft_bucket"),
    )
    .agg(
        F.avg("sqft").alias("avg_sqft"),
        F.sum(F.when(F.col("city") == "Tokyo", 1).otherwise(0)).alias("tokyo_count"),
    )
)

result = (
    agg
    .filter(F.col("tokyo_count") > 60)
    .select("sqft_bucket", "avg_sqft")
)
