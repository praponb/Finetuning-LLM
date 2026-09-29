from pyspark.sql import functions as F

result = (
    df_store_sales.alias("store_sales")
    .join(df_products.alias("products"), F.col("products.id") == F.col("store_sales.product_id"), "left")
    .join(df_stores.alias("stores"), F.col("stores.id") == F.col("store_sales.store_id"), "left")
    .filter(F.col("store_sales.units") > 37)
    .select(
        F.col("store_sales.id"),
        F.col("store_sales.product_id"),
        F.col("products.price"),
        F.col("products.stock_qty"),
        F.col("stores.region"),
    )
    .orderBy(F.col("product_id").desc())
)
