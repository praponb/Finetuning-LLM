from pyspark.sql import functions as F

store_summary = (
    df_inventory
    .filter(F.col("product_id") == 9)
    .groupBy("store_id")
    .agg(F.min("last_restock").alias("min_last_restock"))
)

result = (
    df_stores.alias("s")
    .join(store_summary.alias("ss"), F.col("s.id") == F.col("ss.store_id"), "inner")
    .select(
        F.col("s.id"),
        F.col("s.store_name"),
        F.col("s.region"),
        F.col("ss.min_last_restock"),
    )
)
