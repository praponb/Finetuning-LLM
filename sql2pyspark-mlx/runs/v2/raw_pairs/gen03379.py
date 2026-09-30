from pyspark.sql import functions as F

result = (
    df_inventory.alias("inventory")
    .join(df_products.alias("products"), F.col("inventory.product_id") == F.col("products.id"), "inner")
    .join(df_stores.alias("stores"), F.col("inventory.store_id") == F.col("stores.id"), "outer")
    .select(
        F.col("inventory.quantity"),
        F.col("products.category"),
        F.col("products.stock_qty"),
        F.col("stores.opened_date"),
        F.col("stores.region"),
    )
    .orderBy(F.col("stock_qty").desc())
)
