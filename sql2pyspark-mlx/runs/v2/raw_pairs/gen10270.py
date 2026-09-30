from pyspark.sql import functions as F

result = (
    df_products.alias("products")
    .join(df_store_sales.alias("store_sales"), F.col("store_sales.product_id") == F.col("products.id"), "left")
    .select(
        F.col("products.product_name"),
        F.col("products.price"),
        F.col("store_sales.product_id"),
    )
    .orderBy("price")
)
