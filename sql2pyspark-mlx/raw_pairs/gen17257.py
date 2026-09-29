from pyspark.sql import functions as F

result = (
    df_products.alias("products")
    .join(df_store_sales.alias("store_sales"), F.col("products.id") == F.col("store_sales.product_id"), "left")
    .select(F.col("products.price"), F.col("store_sales.store_id"))
)
