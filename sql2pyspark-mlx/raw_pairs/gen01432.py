from pyspark.sql import functions as F

result = (
    df_store_sales.alias("store_sales")
    .join(df_stores.alias("stores"), F.col("store_sales.store_id") == F.col("stores.id"), "outer")
    .join(df_products.alias("products"), F.col("store_sales.product_id") == F.col("products.id"), "outer")
    .filter((F.length(F.col("products.product_name")) > 12) & (F.col("store_sales.sale_date") >= "2024-01-09"))
    .select(
        F.col("store_sales.id"),
        F.col("store_sales.revenue"),
        F.col("store_sales.store_id"),
        F.col("stores.sqft"),
        F.col("stores.opened_date"),
        F.col("products.launch_date"),
    )
)
