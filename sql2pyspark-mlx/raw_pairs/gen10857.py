from pyspark.sql import functions as F

order_totals = (
    df_shipments
    .filter(F.col("order_id").isin([34, 36]))
    .groupBy("order_id")
    .agg(F.sum("cost").alias("total_cost"))
)

result = (
    df_orders.alias("orders")
    .join(order_totals.alias("t"), F.col("orders.id") == F.col("t.order_id"), "left")
    .filter(F.col("t.total_cost") >= 480)
    .select(F.col("orders.id"), F.col("orders.shipping_fee"), F.col("t.total_cost"))
)
