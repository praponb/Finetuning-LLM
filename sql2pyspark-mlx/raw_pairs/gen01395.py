from pyspark.sql import functions as F

result = (
    df_inventory.alias("inventory")
    .join(df_stores.alias("stores"), F.col("stores.id") == F.col("inventory.store_id"), "inner")
    .filter(F.col("inventory.quantity") < 207)
    .select(
        F.col("inventory.product_id"),
        F.col("inventory.last_restock"),
        F.col("stores.region"),
    )
)
