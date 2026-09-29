from pyspark.sql import functions as F

result = (
    df_inventory.alias("inventory")
    .join(df_stores.alias("stores"), F.col("inventory.store_id") == F.col("stores.id"), "inner")
    .join(df_products.alias("products"), F.col("inventory.product_id") == F.col("products.id"), "inner")
    .filter(F.col("inventory.store_id").isin([1, 4]))
    .select(
        F.col("inventory.id"),
        F.col("inventory.quantity"),
        F.col("stores.opened_date"),
        F.col("products.stock_qty"),
        F.col("products.launch_date"),
    )
    .orderBy(F.col("quantity").desc(), "id")
    .limit(50)
)
