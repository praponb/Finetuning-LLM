from pyspark.sql import functions as F

store_summary = (
    df_inventory
    .groupBy("store_id")
    .agg(
        F.count("*").alias("cnt"),
        F.max("last_restock").alias("latest_last_restock"),
    )
)

result = (
    df_stores.alias("stores")
    .join(store_summary.alias("store_summary"), F.col("stores.id") == F.col("store_summary.store_id"), "inner")
    .filter(F.col("store_summary.cnt") >= 6)
    .select(
        F.col("stores.id"),
        F.col("stores.opened_date"),
        F.col("store_summary.cnt"),
        F.col("store_summary.latest_last_restock"),
    )
    .orderBy(F.col("latest_last_restock").desc())
)
