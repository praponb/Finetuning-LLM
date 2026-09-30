from pyspark.sql import functions as F

result = (
    df_store_sales.alias("store_sales")
    .join(df_products.alias("products"), F.col("store_sales.product_id") == F.col("products.id"), "left")
    .join(df_stores.alias("stores"), F.col("store_sales.store_id") == F.col("stores.id"), "left")
    .filter((F.length(F.col("products.product_name")) < 6) & (F.col("store_sales.revenue") >= 2700))
    .select(
        F.col("store_sales.id"),
        F.col("store_sales.revenue"),
        F.col("products.product_name"),
        F.col("products.stock_qty"),
        F.col("stores.city"),
        F.col("stores.sqft"),
    )
)
