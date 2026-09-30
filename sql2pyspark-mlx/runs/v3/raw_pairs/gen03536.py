from pyspark.sql import functions as F

result = (
    df_products.alias("products")
    .join(df_store_sales.alias("store_sales"), F.col("store_sales.product_id") == F.col("products.id"), "left")
    .groupBy(F.col("products.id"), F.col("products.brand"))
    .agg(F.count(F.col("store_sales.id")).alias("store_sale_count"))
    .orderBy(F.col("store_sale_count").desc())
)
