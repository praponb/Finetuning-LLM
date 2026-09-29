from pyspark.sql import functions as F

product_stats = (
    df_order_items
    .filter(F.col("unit_price") <= 160)
    .groupBy("product_id")
    .agg(
        F.countDistinct("unit_price").alias("unique_products"),
        F.round(F.avg("quantity"), 2).alias("avg_quantity"),
    )
)

result = (
    df_products.alias("p")
    .join(product_stats.alias("ps"), F.col("p.id") == F.col("ps.product_id"), "inner")
    .filter(F.col("ps.avg_quantity") >= 51)
    .select(
        F.col("p.id"),
        F.col("p.price"),
        F.col("ps.unique_products"),
        F.col("ps.avg_quantity"),
    )
    .orderBy(F.col("avg_quantity").desc(), "price")
)
