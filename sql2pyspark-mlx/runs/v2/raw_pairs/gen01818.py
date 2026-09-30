from pyspark.sql import functions as F

result = (
    df_inventory.alias("inventory")
    .join(df_products.alias("products"), F.col("inventory.product_id") == F.col("products.id"), "left")
    .join(df_stores.alias("stores"), F.col("inventory.store_id") == F.col("stores.id"), "left")
    .select(
        F.col("inventory.store_id"),
        F.col("products.stock_qty"),
        F.col("stores.store_name"),
    )
    .orderBy("store_id", F.col("store_name").desc())
)
