from pyspark.sql import functions as F

payments_by_order = (
    df_payments
    .filter(F.col("status").isin(["SUCCESS", "REFUNDED"]))
    .groupBy("order_id")
    .agg(
        F.sort_array(F.collect_set("status")).alias("status_list"),
        F.avg("amount").alias("avg_amount"),
    )
)

result = (
    df_orders.alias("o")
    .join(payments_by_order.alias("t"), F.col("o.id") == F.col("t.order_id"), "left")
    .filter(F.col("t.avg_amount") >= 750)
    .select(
        F.col("o.id"),
        F.col("o.channel"),
        F.col("o.shipping_fee"),
        F.col("t.status_list"),
        F.col("t.avg_amount"),
    )
)
