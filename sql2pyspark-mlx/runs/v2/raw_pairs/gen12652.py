from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "left")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("oi.order_id"), "inner")
    .groupBy(F.month(F.col("o.order_date")).alias("order_month"), F.col("o.status"))
    .agg(
        F.round(F.stddev(F.col("oi.quantity")), 2).alias("stddev_quantity"),
        F.count(F.col("oi.quantity")).alias("quantity_count"),
    )
    .orderBy(F.col("order_month").desc())
)
