from pyspark.sql import functions as F

result = (
    df_inventory.alias("inventory")
    .join(df_stores.alias("stores"), F.col("inventory.store_id") == F.col("stores.id"), "left")
    .join(df_products.alias("products"), F.col("inventory.product_id") == F.col("products.id"), "left")
    .filter(F.col("inventory.store_id").isin([1, 2, 3, 5]))
    .select(
        F.col("inventory.store_id"),
        F.col("stores.store_name"),
        F.col("products.price"),
        F.col("products.stock_qty"),
    )
    .orderBy(F.col("store_id").desc(), "stock_qty")
)
