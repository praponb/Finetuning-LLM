from pyspark.sql import functions as F

result = (
    df_inventory.alias("inventory")
    .join(df_stores.alias("stores"), F.col("stores.id") == F.col("inventory.store_id"), "left")
    .select(
        F.col("inventory.product_id"),
        F.col("inventory.store_id"),
        F.col("inventory.quantity"),
        F.col("stores.sqft"),
        F.col("stores.city"),
    )
)
