from pyspark.sql import functions as F

inventory_by_store = (
    df_inventory
    .groupBy("store_id")
    .agg(
        (F.max("quantity") - F.min("quantity")).alias("quantity_range"),
        F.avg("quantity").alias("avg_quantity"),
    )
)

result = (
    df_stores.alias("stores")
    .join(inventory_by_store.alias("inventory_by_store"), F.col("stores.id") == F.col("inventory_by_store.store_id"), "inner")
    .select(
        F.col("stores.id"),
        F.col("stores.city"),
        F.col("inventory_by_store.quantity_range"),
        F.col("inventory_by_store.avg_quantity"),
    )
    .orderBy(F.col("city").desc())
)
