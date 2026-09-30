from pyspark.sql import functions as F

result = (
    df_products.alias("products")
    .join(df_store_sales.alias("store_sales"), F.col("products.id") == F.col("store_sales.product_id"), "left_semi")
    .select(
        F.col("products.id"),
        F.col("products.cost"),
        F.col("products.product_name"),
    )
)
