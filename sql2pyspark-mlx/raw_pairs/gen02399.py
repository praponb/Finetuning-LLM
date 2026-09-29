from pyspark.sql import functions as F

product_stats = (
    df_store_sales
    .groupBy("product_id")
    .agg(
        F.round(F.avg("revenue"), 2).alias("avg_revenue"),
        F.count("*").alias("cnt"),
    )
)

result = (
    df_products.alias("p")
    .join(product_stats.alias("ps"), F.col("p.id") == F.col("ps.product_id"), "left")
    .filter(F.col("ps.cnt") >= 6)
    .select(F.col("p.id"), F.col("p.cost"), F.col("ps.avg_revenue"), F.col("ps.cnt"))
)
