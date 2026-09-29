from pyspark.sql import functions as F

result = (
    df_products.alias("products")
    .join(df_store_sales.alias("store_sales"), F.col("products.id") == F.col("store_sales.product_id"), "inner")
    .filter(F.col("store_sales.revenue") >= 1900)
    .groupBy(F.col("products.id"), F.col("products.category"))
    .agg(F.count(F.col("store_sales.id")).alias("store_sale_count"))
    .orderBy("store_sale_count", "id", "category")
    .limit(50)
)
