from pyspark.sql import functions as F

payments_by_order = (
    df_payments
    .groupBy("order_id")
    .agg(F.sort_array(F.collect_set("status")).alias("status_list"))
)

result = (
    df_orders.alias("o")
    .join(payments_by_order.alias("t"), F.col("o.id") == F.col("t.order_id"), "inner")
    .select(F.col("o.shipping_fee"), F.col("o.channel"), F.col("t.status_list"))
    .orderBy("channel")
)
