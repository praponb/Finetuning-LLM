from pyspark.sql import functions as F

inventory_by_store = (
    df_inventory
    .filter(F.col("last_restock") < "2024-03-19")
    .groupBy("store_id")
    .agg(
        F.max("quantity").alias("max_quantity"),
        F.countDistinct("quantity").alias("unique_stores"),
    )
)

result = (
    df_stores.alias("s")
    .join(inventory_by_store.alias("ibs"), F.col("s.id") == F.col("ibs.store_id"), "left")
    .select(
        F.col("s.id"),
        F.col("s.store_name"),
        F.col("s.opened_date"),
        F.col("ibs.max_quantity"),
        F.col("ibs.unique_stores"),
    )
)
