from pyspark.sql import functions as F

result = (
    df_inventory.alias("inventory")
    .join(df_stores.alias("stores"), F.col("inventory.store_id") == F.col("stores.id"), "inner")
    .join(df_products.alias("products"), F.col("inventory.product_id") == F.col("products.id"), "inner")
    .filter(F.col("stores.store_name").isNotNull())
    .select(
        F.col("inventory.id"),
        F.col("inventory.quantity"),
        F.col("inventory.last_restock"),
        F.col("stores.city"),
        F.col("stores.store_name"),
        F.col("products.stock_qty"),
    )
    .orderBy("city")
)
