from pyspark.sql import functions as F

product_stats = (
    df_store_sales
    .groupBy("product_id")
    .agg(F.avg("revenue").alias("avg_revenue"))
)

result = (
    df_products.alias("p")
    .join(product_stats.alias("t"), F.col("p.id") == F.col("t.product_id"), "inner")
    .filter(F.col("t.avg_revenue") >= 3200)
    .select(
        F.col("p.id"),
        F.col("p.launch_date"),
        F.col("p.stock_qty"),
        F.col("t.avg_revenue"),
    )
    .orderBy(F.col("launch_date").desc())
)
