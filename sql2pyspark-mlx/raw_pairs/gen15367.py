from pyspark.sql import functions as F

result = (
    df_inventory.alias("inventory")
    .join(df_products.alias("products"), F.col("inventory.product_id") == F.col("products.id"), "inner")
    .join(df_stores.alias("stores"), F.col("inventory.store_id") == F.col("stores.id"), "inner")
    .filter((F.col("inventory.last_restock") >= "2025-03-06") & (F.col("stores.sqft") > 11317))
    .select(
        F.col("inventory.id"),
        F.col("inventory.quantity"),
        F.col("inventory.last_restock"),
        F.col("products.supplier_id"),
        F.col("products.category"),
        F.col("stores.store_name"),
        F.col("stores.region"),
    )
)
