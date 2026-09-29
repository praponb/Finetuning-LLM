from pyspark.sql import functions as F

payments_by_order = (
    df_payments
    .filter(F.col("amount") > 1210)
    .groupBy("order_id")
    .agg(F.sort_array(F.collect_set("method")).alias("method_list"))
)

result = (
    df_orders.alias("o")
    .join(payments_by_order.alias("t"), F.col("o.id") == F.col("t.order_id"), "left")
    .select(F.col("o.id"), F.col("o.channel"), F.col("t.method_list"))
    .orderBy("channel")
)
