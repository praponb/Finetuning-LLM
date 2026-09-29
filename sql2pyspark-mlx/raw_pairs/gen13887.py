from pyspark.sql import functions as F

product_summary = (
    df_order_items
    .groupBy("product_id")
    .agg(
        F.count("unit_price").alias("unit_price_count"),
        F.round(F.avg("quantity"), 2).alias("avg_quantity"),
    )
)

result = (
    df_products.alias("p")
    .join(product_summary.alias("ps"), F.col("p.id") == F.col("ps.product_id"), "inner")
    .select(
        F.col("p.id"),
        F.col("p.category"),
        F.col("ps.unit_price_count"),
        F.col("ps.avg_quantity"),
    )
    .orderBy(F.col("avg_quantity").desc())
)
