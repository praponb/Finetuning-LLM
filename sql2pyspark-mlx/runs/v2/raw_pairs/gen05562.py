from pyspark.sql import functions as F

inventory_by_store = (
    df_inventory
    .filter(F.col("store_id") == 6)
    .groupBy("store_id")
    .agg(
        F.countDistinct("quantity").alias("unique_stores"),
        F.max("last_restock").alias("latest_last_restock"),
    )
)

result = (
    df_stores.alias("s")
    .join(inventory_by_store.alias("ibs"), F.col("s.id") == F.col("ibs.store_id"), "inner")
    .select(
        F.col("s.id"),
        F.col("s.store_name"),
        F.col("s.region"),
        F.col("ibs.unique_stores"),
        F.col("ibs.latest_last_restock"),
    )
    .orderBy("store_name")
)
