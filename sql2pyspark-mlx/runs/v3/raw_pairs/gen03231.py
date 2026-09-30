from pyspark.sql import functions as F

product_summary = (
    df_store_sales
    .groupBy("product_id")
    .agg(F.count("revenue").alias("revenue_count"))
)

result = (
    df_products.alias("p")
    .join(product_summary.alias("ps"), F.col("p.id") == F.col("ps.product_id"), "inner")
    .filter(F.col("ps.revenue_count") > 6)
    .select(F.col("p.stock_qty"), F.col("p.price"), F.col("ps.revenue_count"))
    .orderBy("stock_qty", F.col("price").desc())
)
