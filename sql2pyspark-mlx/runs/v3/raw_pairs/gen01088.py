from pyspark.sql import functions as F

order_stats = (
    df_payments
    .groupBy("order_id")
    .agg(F.sort_array(F.collect_set("method")).alias("method_list"))
)

result = (
    df_orders.alias("o")
    .join(order_stats.alias("t"), F.col("o.id") == F.col("t.order_id"), "left")
    .select(
        F.col("o.id"),
        F.col("o.amount"),
        F.col("o.order_date"),
        F.col("t.method_list"),
    )
)
