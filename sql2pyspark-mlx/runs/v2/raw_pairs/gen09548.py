from pyspark.sql import functions as F

store_summary = (
    df_store_sales
    .groupBy("store_id")
    .agg(
        F.countDistinct("revenue").alias("unique_products"),
        F.max("revenue").alias("max_revenue"),
    )
)

result = (
    df_stores.alias("s")
    .join(store_summary.alias("ss"), F.col("s.id") == F.col("ss.store_id"), "inner")
    .select(
        F.col("s.id"),
        F.col("s.opened_date"),
        F.col("ss.unique_products"),
        F.col("ss.max_revenue"),
    )
    .orderBy("unique_products")
)
