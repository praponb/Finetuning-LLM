from pyspark.sql import functions as F

result = (
    df_stores.alias("stores")
    .join(df_inventory.alias("inventory"), F.col("inventory.store_id") == F.col("stores.id"), "left")
    .filter(F.col("inventory.store_id").isin([2, 3, 5, 6]))
    .groupBy(F.col("stores.id"), F.col("stores.city"))
    .agg(
        F.count(F.col("inventory.id")).alias("inventory_count"),
        F.max(F.col("inventory.quantity")).alias("max_quantity"),
    )
)
