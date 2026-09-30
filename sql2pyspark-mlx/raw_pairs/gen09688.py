from pyspark.sql import functions as F

product_stats = (
    df_store_sales
    .groupBy("product_id")
    .agg(F.min("revenue").alias("min_revenue"))
)

result = (
    df_products.alias("p")
    .join(product_stats.alias("ps"), F.col("p.id") == F.col("ps.product_id"), "inner")
    .filter(F.col("ps.min_revenue") > 1600)
    .select(F.col("p.brand"), F.col("ps.min_revenue"))
    .orderBy(F.col("min_revenue").desc())
)
