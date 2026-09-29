from pyspark.sql import functions as F

inventory_by_store = (
    df_inventory
    .groupBy("store_id")
    .agg(F.max("last_restock").alias("last_last_restock"))
)

result = (
    df_stores.alias("s")
    .join(inventory_by_store.alias("ibs"), F.col("s.id") == F.col("ibs.store_id"), "inner")
    .select(F.col("s.region"), F.col("s.store_name"), F.col("ibs.last_last_restock"))
    .orderBy("last_last_restock", "store_name")
)
