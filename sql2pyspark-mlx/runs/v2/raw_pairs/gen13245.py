from pyspark.sql import functions as F

inventory_by_store = (
    df_inventory
    .filter(F.col("last_restock") < "2023-10-13")
    .groupBy("store_id")
    .agg(F.count("quantity").alias("quantity_count"))
)

result = (
    df_stores.alias("stores")
    .join(inventory_by_store.alias("inventory_by_store"), F.col("stores.id") == F.col("inventory_by_store.store_id"), "inner")
    .select(
        F.col("stores.id"),
        F.col("stores.opened_date"),
        F.col("inventory_by_store.quantity_count"),
    )
    .orderBy("quantity_count", "id")
)
