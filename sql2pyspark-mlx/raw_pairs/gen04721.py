from pyspark.sql import functions as F

order_totals = (
    df_order_items
    .filter(F.col("unit_price") < 230)
    .groupBy("order_id")
    .agg(F.sum((F.col("unit_price") * F.col("quantity"))).alias("revenue"))
)

result = (
    df_orders.alias("o")
    .join(order_totals.alias("t"), F.col("o.id") == F.col("t.order_id"), "left")
    .select(F.col("o.id"), F.col("o.amount"), F.col("t.revenue"))
    .orderBy(F.col("amount").desc())
)
