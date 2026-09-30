from pyspark.sql import functions as F

order_totals = (
    df_payments
    .filter(F.col("paid_at") >= "2024-01-01")
    .groupBy("order_id")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("status"))).alias("status_values"),
    )
)

result = (
    df_orders.alias("o")
    .join(order_totals.alias("ot"), F.col("o.id") == F.col("ot.order_id"), "left")
    .select(F.col("o.customer_id"), F.col("ot.status_values"))
)
