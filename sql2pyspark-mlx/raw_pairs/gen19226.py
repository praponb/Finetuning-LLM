from pyspark.sql import functions as F

product_stats = (
    df_order_items
    .filter(F.col("unit_price") > 360)
    .groupBy("product_id")
    .agg(
        F.round(F.stddev("unit_price"), 2).alias("stddev_unit_price"),
        F.avg("unit_price").alias("avg_unit_price"),
    )
)

result = (
    df_products.alias("p")
    .join(product_stats.alias("ps"), F.col("p.id") == F.col("ps.product_id"), "left")
    .select(
        F.col("p.id"),
        F.col("p.supplier_id"),
        F.col("ps.stddev_unit_price"),
        F.col("ps.avg_unit_price"),
    )
    .orderBy("avg_unit_price", F.col("supplier_id").desc())
)
