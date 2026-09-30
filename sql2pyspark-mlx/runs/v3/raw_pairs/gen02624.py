from pyspark.sql import functions as F

store_summary = (
    df_store_sales
    .groupBy("store_id")
    .agg(
        F.min("sale_date").alias("min_sale_date"),
        F.avg("revenue").alias("avg_revenue"),
    )
)

result = (
    df_stores.alias("s")
    .join(store_summary.alias("ss"), F.col("s.id") == F.col("ss.store_id"), "inner")
    .filter(F.col("ss.avg_revenue") >= 2600)
    .select(
        F.col("s.id"),
        F.col("s.store_name"),
        F.col("ss.min_sale_date"),
        F.col("ss.avg_revenue"),
    )
    .orderBy("id")
    .limit(5)
)
