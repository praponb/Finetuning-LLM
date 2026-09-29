from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy("product_id")
    .agg(
        F.round(F.avg("revenue"), 2).alias("avg_revenue"),
        F.count("revenue").alias("revenue_count"),
    )
    .filter(F.col("avg_revenue") < 51)
    .select("product_id", "avg_revenue", "revenue_count")
)
