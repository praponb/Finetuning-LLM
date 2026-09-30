from pyspark.sql import functions as F

product_stats = (
    df_store_sales
    .groupBy("product_id")
    .agg(
        F.sum("revenue").alias("total_revenue"),
        (F.max("revenue") - F.min("revenue")).alias("revenue_range"),
    )
)

result = (
    df_products.alias("p")
    .join(product_stats.alias("t"), F.col("p.id") == F.col("t.product_id"), "left")
    .select(F.col("p.stock_qty"), F.col("t.total_revenue"), F.col("t.revenue_range"))
    .orderBy(F.col("total_revenue").desc(), "stock_qty")
)
