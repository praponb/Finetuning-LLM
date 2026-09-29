from pyspark.sql import functions as F

product_summary = (
    df_order_items
    .filter(F.col("product_id") == 12)
    .groupBy("product_id")
    .agg(
        F.round(F.avg("quantity"), 2).alias("avg_quantity"),
        F.round(F.stddev("quantity"), 2).alias("stddev_quantity"),
    )
)

result = (
    df_products.alias("p")
    .join(product_summary.alias("ps"), F.col("p.id") == F.col("ps.product_id"), "inner")
    .filter(F.col("ps.stddev_quantity") > 53)
    .select(F.col("p.category"), F.col("ps.avg_quantity"), F.col("ps.stddev_quantity"))
)
