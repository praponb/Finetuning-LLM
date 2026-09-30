from pyspark.sql import functions as F

result = (
    df_inventory.alias("inventory")
    .join(df_stores.alias("stores"), F.col("inventory.store_id") == F.col("stores.id"), "right")
    .join(df_products.alias("products"), F.col("inventory.product_id") == F.col("products.id"), "inner")
    .filter(F.col("stores.opened_date") <= "2025-07-01")
    .select(
        F.col("inventory.id"),
        F.col("inventory.quantity"),
        F.col("inventory.store_id"),
        F.col("stores.opened_date"),
        F.col("products.launch_date"),
        F.date_format(F.col("inventory.last_restock"), "yyyy-MM").alias("last_restock_label"),
    )
)
