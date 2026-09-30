from pyspark.sql import functions as F

result = (
    df_inventory.alias("inventory")
    .join(df_stores.alias("stores"), F.col("stores.id") == F.col("inventory.store_id"), "inner")
    .select(
        F.col("inventory.id"),
        F.col("inventory.last_restock"),
        F.col("stores.sqft"),
        F.col("stores.city"),
        F.col("stores.store_name"),
    )
)
