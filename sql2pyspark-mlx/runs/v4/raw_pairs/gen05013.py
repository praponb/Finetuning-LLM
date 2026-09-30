from pyspark.sql import functions as F

store_summary = (
    df_inventory
    .groupBy("store_id")
    .agg(
        F.count("*").alias("total_count"),
        (F.max("quantity") - F.min("quantity")).alias("quantity_range"),
    )
)

result = (
    df_stores.alias("stores")
    .join(store_summary.alias("store_summary"), F.col("stores.id") == F.col("store_summary.store_id"), "inner")
    .filter(F.col("store_summary.total_count") >= 8)
    .select(
        F.col("stores.city"),
        F.col("stores.store_name"),
        F.col("store_summary.total_count"),
        F.col("store_summary.quantity_range"),
    )
    .orderBy(F.col("total_count").desc(), "city")
)
