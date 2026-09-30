from pyspark.sql import functions as F

store_summary = (
    df_store_sales
    .groupBy("store_id")
    .agg(
        F.count("revenue").alias("revenue_count"),
        F.round(F.avg("revenue"), 2).alias("avg_revenue"),
    )
)

result = (
    df_stores.alias("stores")
    .join(store_summary.alias("StoreSummary"), F.col("stores.id") == F.col("StoreSummary.store_id"), "inner")
    .select(
        F.col("stores.city"),
        F.col("StoreSummary.revenue_count"),
        F.col("StoreSummary.avg_revenue"),
    )
    .orderBy("revenue_count")
)
