from pyspark.sql import functions as F

product_stats = (
    df_order_items
    .groupBy("product_id")
    .agg(
        F.countDistinct("unit_price").alias("unique_orders"),
        F.avg("quantity").alias("avg_quantity"),
    )
)

result = (
    df_products.alias("p")
    .join(product_stats.alias("ps"), F.col("p.id") == F.col("ps.product_id"), "inner")
    .select(
        F.col("p.id"),
        F.col("p.category"),
        F.col("ps.unique_orders"),
        F.col("ps.avg_quantity"),
    )
    .orderBy(F.col("avg_quantity").desc(), F.col("unique_orders").desc(), "id")
    .limit(3)
)
