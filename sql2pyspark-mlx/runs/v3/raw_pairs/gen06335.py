from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "left")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("oi.order_id"), "left")
    .filter(F.col("p.supplier_id").isin([6, 9]))
    .groupBy(F.col("o.channel"))
    .agg(
        F.avg(F.col("oi.quantity")).alias("avg_quantity"),
        F.round(F.stddev(F.col("oi.unit_price")), 2).alias("stddev_unit_price"),
    )
    .filter(F.col("stddev_unit_price") >= 73)
)
