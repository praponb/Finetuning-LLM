from pyspark.sql import functions as F

store_summary = (
    df_store_sales
    .groupBy("store_id")
    .agg(
        F.min("sale_date").alias("min_sale_date"),
        F.min("revenue").alias("min_revenue"),
    )
)

result = (
    df_stores.alias("stores")
    .join(store_summary.alias("store_summary"), F.col("stores.id") == F.col("store_summary.store_id"), "inner")
    .filter(F.col("store_summary.min_revenue") > 1000)
    .select(
        F.col("stores.id"),
        F.col("stores.opened_date"),
        F.col("store_summary.min_sale_date"),
        F.col("store_summary.min_revenue"),
    )
)
