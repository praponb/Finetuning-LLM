from pyspark.sql import functions as F

result = (
    df_products.alias("products")
    .join(df_store_sales.filter(F.col("store_id").isin([1, 2, 4, 6])).alias("store_sales"), F.col("products.id") == F.col("store_sales.product_id"), "left_semi")
    .select(F.col("products.id"), F.col("products.category"))
    .orderBy(F.col("category").desc())
)
