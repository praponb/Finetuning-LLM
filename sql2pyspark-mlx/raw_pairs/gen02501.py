from pyspark.sql import functions as F

order_totals = (
    df_payments
    .groupBy("order_id")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("method"))).alias("method_values"),
        F.min("amount").alias("min_amount"),
    )
)

result = (
    df_orders.alias("orders")
    .join(order_totals.alias("order_totals"), F.col("orders.id") == F.col("order_totals.order_id"), "inner")
    .filter(F.col("order_totals.min_amount") >= 540)
    .select(
        F.col("orders.channel"),
        F.col("order_totals.method_values"),
        F.col("order_totals.min_amount"),
    )
    .orderBy("channel")
)
