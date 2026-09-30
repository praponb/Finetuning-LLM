from pyspark.sql import functions as F

product_stats = (
    df_store_sales
    .groupBy("product_id")
    .agg(F.count("*").alias("cnt"))
)

result = (
    df_products.alias("p")
    .join(product_stats.alias("ps"), F.col("p.id") == F.col("ps.product_id"), "inner")
    .filter(F.col("ps.cnt") >= 4)
    .select(F.col("p.id"), F.col("p.launch_date"), F.col("ps.cnt"))
)
