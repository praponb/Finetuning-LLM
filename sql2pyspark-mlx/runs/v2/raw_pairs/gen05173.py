from pyspark.sql import functions as F

result = (
    df_inventory.alias("inventory")
    .join(df_products.alias("products"), F.col("inventory.product_id") == F.col("products.id"), "inner")
    .join(df_stores.alias("stores"), F.col("inventory.store_id") == F.col("stores.id"), "right")
    .filter(F.col("inventory.store_id").isin([2, 3]))
    .select(
        F.col("inventory.id"),
        F.col("inventory.quantity"),
        F.col("products.product_name"),
        F.col("stores.store_name"),
    )
    .orderBy("product_name")
)
