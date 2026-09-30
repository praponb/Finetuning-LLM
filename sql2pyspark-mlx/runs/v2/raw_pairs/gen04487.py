from pyspark.sql import functions as F

product_stats = (
    df_store_sales
    .groupBy("product_id")
    .agg(
        F.sum("revenue").alias("total_revenue"),
        F.avg("revenue").alias("avg_revenue"),
    )
)

result = (
    df_products.alias("p")
    .join(product_stats.alias("ps"), F.col("p.id") == F.col("ps.product_id"), "left")
    .filter(F.col("ps.total_revenue") > 10000)
    .select(F.col("p.category"), F.col("ps.total_revenue"), F.col("ps.avg_revenue"))
    .orderBy(F.col("total_revenue").desc(), "category")
)
