from pyspark.sql import functions as F

result = (
    df_stores.alias("stores")
    .join(df_inventory.alias("inventory"), F.col("inventory.store_id") == F.col("stores.id"), "left")
    .filter(~F.col("stores.store_name").like("Si%"))
    .select(
        F.col("stores.sqft"),
        F.col("stores.city"),
        F.col("inventory.last_restock"),
        F.col("inventory.product_id"),
    )
)
