from pyspark.sql import functions as F

result = (
    df_products.alias("products")
    .join(df_store_sales.alias("store_sales"), F.col("products.id") == F.col("store_sales.product_id"), "left")
    .filter((F.col("products.price") <= 600) & F.col("store_sales.product_id").isin([5, 8, 9, 10]))
    .select(
        F.col("products.product_name"),
        F.col("products.category"),
        F.col("store_sales.product_id"),
    )
)
