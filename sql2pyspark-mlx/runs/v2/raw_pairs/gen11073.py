from pyspark.sql import functions as F

result = (
    df_inventory.alias("inventory")
    .join(df_stores.alias("stores"), F.col("stores.id") == F.col("inventory.store_id"), "inner")
    .filter(F.col("inventory.last_restock") <= "2025-10-01")
    .select(F.col("inventory.id"), F.col("inventory.product_id"), F.col("stores.city"))
)
