from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_order_items.alias("oi"), F.col("oi.product_id") == F.col("p.id"), "left")
    .filter(F.col("p.cost").isNotNull())
    .groupBy(F.col("p.id"), F.col("p.brand"))
    .agg(
        F.count(F.col("oi.id")).alias("order_item_count"),
        F.round(F.stddev(F.col("oi.unit_price")), 2).alias("stddev_unit_price"),
    )
    .orderBy(F.col("brand").desc())
)
