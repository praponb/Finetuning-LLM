from pyspark.sql import functions as F

payments_by_order = (
    df_payments
    .filter(F.col("order_id") == 16)
    .groupBy("order_id")
    .agg(F.sort_array(F.collect_set("status")).alias("status_list"))
)

result = (
    df_orders.alias("o")
    .join(payments_by_order.alias("pbo"), F.col("o.id") == F.col("pbo.order_id"), "left")
    .select(F.col("o.store_id"), F.col("o.discount"), F.col("pbo.status_list"))
)
