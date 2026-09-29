from pyspark.sql import functions as F

order_stats = (
    df_payments
    .groupBy("order_id")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("status"))).alias("status_values"),
    )
)

result = (
    df_orders.alias("o")
    .join(order_stats.alias("O"), F.col("o.id") == F.col("O.order_id"), "inner")
    .select(F.col("o.customer_id"), F.col("o.order_date"), F.col("O.status_values"))
)
