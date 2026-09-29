from pyspark.sql import functions as F

result = (
    df_store_sales.alias("store_sales")
    .join(df_products.alias("products"), F.col("store_sales.product_id") == F.col("products.id"), "right")
    .filter(F.col("store_sales.revenue").between(2700, 3400))
    .groupBy(F.col("products.brand"))
    .agg(F.count("*").alias("store_sale_count"))
    .orderBy(F.col("brand").desc(), F.col("store_sale_count").desc())
)
