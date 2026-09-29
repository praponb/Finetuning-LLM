from pyspark.sql import functions as F

product_stats = (
    df_store_sales
    .groupBy("product_id")
    .agg(F.sum("revenue").alias("total_revenue"))
)

result = (
    df_products.alias("products")
    .join(product_stats.alias("ProductStats"), F.col("products.id") == F.col("ProductStats.product_id"), "left")
    .filter(F.col("ProductStats.total_revenue") > 20000)
    .select(F.col("products.product_name"), F.col("ProductStats.total_revenue"))
)
