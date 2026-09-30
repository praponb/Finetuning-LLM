from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("oi.order_id"), "right")
    .join(df_products.alias("p"), F.col("p.id") == F.col("oi.product_id"), "right")
    .filter(F.col("p.price") > 730)
    .groupBy(F.col("p.brand"))
    .agg(
        F.count(F.col("oi.quantity")).alias("quantity_count"),
        F.round(F.stddev(F.col("oi.unit_price")), 2).alias("stddev_unit_price"),
    )
    .orderBy(F.col("quantity_count").desc())
)
