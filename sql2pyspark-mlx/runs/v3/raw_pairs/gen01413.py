from pyspark.sql import functions as F

store_stats = (
    df_inventory
    .filter(F.col("last_restock").between("2024-10-01", "2025-03-01"))
    .groupBy("store_id")
    .agg(F.min("last_restock").alias("min_last_restock"))
)

result = (
    df_stores.alias("s")
    .join(store_stats.alias("t"), F.col("s.id") == F.col("t.store_id"), "left")
    .select(F.col("s.id"), F.col("s.store_name"), F.col("t.min_last_restock"))
)
