from pyspark.sql import functions as F

product_stats = (
    df_order_items
    .filter(F.col("order_id") == 8)
    .groupBy("product_id")
    .agg(
        F.round(F.stddev("unit_price"), 2).alias("stddev_unit_price"),
        F.countDistinct("quantity").alias("unique_products"),
    )
)

result = (
    df_products.alias("p")
    .join(product_stats.alias("t"), F.col("p.id") == F.col("t.product_id"), "left")
    .select(
        F.col("p.id"),
        F.col("p.price"),
        F.col("t.stddev_unit_price"),
        F.col("t.unique_products"),
    )
    .orderBy(F.col("price").desc(), "stddev_unit_price")
)
