from pyspark.sql import functions as F

result = (
    df_inventory.alias("inventory")
    .join(df_stores.alias("stores"), F.col("stores.id") == F.col("inventory.store_id"), "inner")
    .join(df_products.alias("products"), F.col("inventory.product_id") == F.col("products.id"), "inner")
    .filter(F.col("stores.region") != "South")
    .select(
        F.col("inventory.id"),
        F.col("inventory.store_id"),
        F.col("stores.store_name"),
        F.col("stores.city"),
        F.col("products.category"),
        F.col("products.cost"),
    )
)
