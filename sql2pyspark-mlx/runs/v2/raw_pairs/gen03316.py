from pyspark.sql import functions as F

inventory_by_store = (
    df_inventory
    .groupBy("store_id")
    .agg(
        F.countDistinct("quantity").alias("unique_products"),
        F.min("last_restock").alias("min_last_restock"),
    )
)

result = (
    df_stores.alias("s")
    .join(inventory_by_store.alias("ibs"), F.col("s.id") == F.col("ibs.store_id"), "left")
    .select(
        F.col("s.opened_date"),
        F.col("ibs.unique_products"),
        F.col("ibs.min_last_restock"),
    )
)
