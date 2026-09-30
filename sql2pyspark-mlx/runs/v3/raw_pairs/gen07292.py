from pyspark.sql import functions as F

result = (
    df_inventory.alias("inventory")
    .join(df_stores.alias("stores"), F.col("stores.id") == F.col("inventory.store_id"), "inner")
    .join(df_products.alias("products"), F.col("inventory.product_id") == F.col("products.id"), "left")
    .select(
        F.col("inventory.product_id"),
        F.col("inventory.last_restock"),
        F.col("stores.region"),
        F.col("stores.store_name"),
        F.col("products.category"),
    )
    .orderBy(F.col("product_id").desc())
)
