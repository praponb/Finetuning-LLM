from pyspark.sql import functions as F

product_stats = (
    df_order_items
    .groupBy("product_id")
    .agg(
        F.avg("quantity").alias("avg_quantity"),
        F.count("*").alias("total_count"),
    )
)

result = (
    df_products.alias("p")
    .join(product_stats.alias("t"), F.col("p.id") == F.col("t.product_id"), "inner")
    .select(F.col("p.supplier_id"), F.col("t.avg_quantity"), F.col("t.total_count"))
)
