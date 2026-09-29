from pyspark.sql import functions as F

product_stats = (
    df_order_items
    .filter(F.col("unit_price") > 170)
    .groupBy("product_id")
    .agg(
        F.count("quantity").alias("quantity_count"),
        F.avg("quantity").alias("avg_quantity"),
    )
)

result = (
    df_products.alias("p")
    .join(product_stats.alias("ps"), F.col("p.id") == F.col("ps.product_id"), "left")
    .select(
        F.col("p.id"),
        F.col("p.category"),
        F.col("ps.quantity_count"),
        F.col("ps.avg_quantity"),
    )
)
