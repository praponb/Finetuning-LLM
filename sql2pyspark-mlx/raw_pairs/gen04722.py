from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "inner")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("oi.order_id"), "inner")
    .groupBy(F.col("o.status"))
    .agg(
        F.avg(F.col("oi.quantity")).alias("avg_quantity"),
        F.round(F.stddev(F.col("oi.unit_price")), 2).alias("stddev_unit_price"),
    )
    .orderBy("status", "stddev_unit_price")
)
