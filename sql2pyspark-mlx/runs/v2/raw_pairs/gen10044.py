from pyspark.sql import functions as F

result = (
    df_products.alias("products")
    .join(df_store_sales.alias("store_sales"), F.col("products.id") == F.col("store_sales.product_id"), "left_anti")
    .select(F.col("products.id"), F.col("products.brand"))
    .orderBy(F.col("id").desc())
)
