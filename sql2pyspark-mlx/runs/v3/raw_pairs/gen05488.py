from pyspark.sql import functions as F

inventory_by_store = (
    df_inventory
    .groupBy("store_id")
    .agg((F.max("quantity") - F.min("quantity")).alias("quantity_range"))
)

result = (
    df_stores.alias("stores")
    .join(inventory_by_store.alias("inventory_by_store"), F.col("stores.id") == F.col("inventory_by_store.store_id"), "inner")
    .select(F.col("stores.store_name"), F.col("inventory_by_store.quantity_range"))
)
