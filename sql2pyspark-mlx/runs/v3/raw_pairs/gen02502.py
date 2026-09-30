from pyspark.sql import functions as F

result = (
    df_inventory.alias("inventory")
    .join(df_products.alias("products"), F.col("inventory.product_id") == F.col("products.id"), "left")
    .filter(F.col("inventory.store_id").isin([1, 2, 5]))
    .select(
        F.col("inventory.last_restock"),
        F.col("products.category"),
        F.col("products.stock_qty"),
    )
    .orderBy(F.col("stock_qty").desc(), "last_restock")
)
