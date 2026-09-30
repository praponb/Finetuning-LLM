from pyspark.sql import functions as F

product_summary = (
    df_store_sales
    .groupBy("product_id")
    .agg(F.avg("revenue").alias("avg_revenue"))
)

result = (
    df_products.alias("products")
    .join(product_summary.alias("product_summary"), F.col("products.id") == F.col("product_summary.product_id"), "inner")
    .select(F.col("products.category"), F.col("product_summary.avg_revenue"))
    .orderBy("category", F.col("avg_revenue").desc())
)
