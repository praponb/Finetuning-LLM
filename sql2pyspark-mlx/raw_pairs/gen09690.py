from pyspark.sql import functions as F

payments_by_order = (
    df_payments
    .filter(F.col("paid_at") >= "2023-11-30")
    .groupBy("order_id")
    .agg(F.sort_array(F.collect_set("status")).alias("status_list"))
)

result = (
    df_orders.alias("o")
    .join(payments_by_order.alias("pbo"), F.col("o.id") == F.col("pbo.order_id"), "inner")
    .select(F.col("o.id"), F.col("o.customer_id"), F.col("pbo.status_list"))
)
