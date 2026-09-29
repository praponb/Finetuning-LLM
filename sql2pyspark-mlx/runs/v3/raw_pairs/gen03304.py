from pyspark.sql import functions as F

order_summary = (
    df_order_items
    .groupBy("order_id")
    .agg(F.sum((F.col("quantity") * F.col("unit_price"))).alias("revenue"))
)

result = (
    df_orders.alias("o")
    .join(order_summary.alias("O"), F.col("o.id") == F.col("O.order_id"), "left")
    .select(F.col("o.id"), F.col("o.amount"), F.col("O.revenue"))
)
