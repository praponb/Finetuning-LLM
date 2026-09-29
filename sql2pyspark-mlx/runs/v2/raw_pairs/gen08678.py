from pyspark.sql import functions as F

result = (
    df_store_sales.alias("store_sales")
    .join(df_products.alias("products"), F.col("store_sales.product_id") == F.col("products.id"), "inner")
    .groupBy(F.col("products.brand"))
    .agg(F.count("*").alias("cnt"))
)
