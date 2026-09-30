from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy("store_id")
    .agg(
        F.count("revenue").alias("revenue_count"),
        F.min("revenue").alias("min_revenue"),
    )
    .filter(F.col("revenue_count") >= 9)
    .select("store_id", "revenue_count", "min_revenue")
    .orderBy(F.col("min_revenue").desc())
)
