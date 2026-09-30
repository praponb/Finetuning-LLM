from pyspark.sql import functions as F

result = (
    df_store_sales.alias("store_sales")
    .join(df_stores.alias("stores"), F.col("stores.id") == F.col("store_sales.store_id"), "inner")
    .join(df_products.alias("products"), F.col("store_sales.product_id") == F.col("products.id"), "left")
    .filter(~F.col("stores.city").isin(["Berlin", "New York", "London"]) & (F.col("store_sales.revenue") <= 1300))
    .select(
        F.col("store_sales.id"),
        F.col("store_sales.product_id"),
        F.col("store_sales.store_id"),
        F.col("stores.store_name"),
        F.col("products.price"),
    )
)
