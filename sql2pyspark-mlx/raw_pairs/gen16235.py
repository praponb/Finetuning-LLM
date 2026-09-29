from pyspark.sql import functions as F

inventory_by_store = (
    df_inventory
    .groupBy("store_id")
    .agg(
        F.min("last_restock").alias("first_last_restock"),
        F.avg("quantity").alias("avg_quantity"),
    )
)

result = (
    df_stores.alias("stores")
    .join(inventory_by_store.alias("inventory_by_store"), F.col("stores.id") == F.col("inventory_by_store.store_id"), "inner")
    .filter(F.col("inventory_by_store.avg_quantity") > 140)
    .select(
        F.col("stores.id"),
        F.col("stores.city"),
        F.col("stores.sqft"),
        F.col("inventory_by_store.first_last_restock"),
        F.col("inventory_by_store.avg_quantity"),
    )
    .orderBy("first_last_restock", "id")
    .limit(10)
)
