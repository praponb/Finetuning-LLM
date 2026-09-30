from pyspark.sql import functions as F

result = (
    df_store_sales.alias("store_sales")
    .join(df_stores.alias("stores"), F.col("store_sales.store_id") == F.col("stores.id"), "left")
    .join(df_products.alias("products"), F.col("store_sales.product_id") == F.col("products.id"), "inner")
    .select(
        F.col("store_sales.id"),
        F.col("store_sales.store_id"),
        F.col("stores.region"),
        F.col("products.cost"),
        F.col("products.stock_qty"),
    )
    .orderBy("stock_qty")
)
