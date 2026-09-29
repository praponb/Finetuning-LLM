from pyspark.sql import functions as F

store_stats = (
    df_store_sales
    .groupBy("store_id")
    .agg(F.avg("revenue").alias("avg_revenue"))
)

result = (
    df_stores.alias("s")
    .join(store_stats.alias("ss"), F.col("s.id") == F.col("ss.store_id"), "inner")
    .select(
        F.col("s.id"),
        F.col("s.opened_date"),
        F.col("s.sqft"),
        F.col("ss.avg_revenue"),
    )
    .orderBy("avg_revenue", F.col("opened_date").desc())
)
