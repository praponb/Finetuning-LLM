from pyspark.sql import functions as F

order_items_by_product = (
    df_order_items
    .groupBy("product_id")
    .agg(
        F.avg("quantity").alias("avg_quantity"),
        F.round(F.stddev("quantity"), 2).alias("stddev_quantity"),
    )
)

result = (
    df_products.alias("p")
    .join(order_items_by_product.alias("O"), F.col("p.id") == F.col("O.product_id"), "left")
    .select(
        F.col("p.id"),
        F.col("p.price"),
        F.col("O.avg_quantity"),
        F.col("O.stddev_quantity"),
    )
    .orderBy(F.col("stddev_quantity").desc())
)
