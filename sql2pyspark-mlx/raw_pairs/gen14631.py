from pyspark.sql import functions as F

order_stats = (
    df_shipments
    .groupBy("order_id")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("carrier"))).alias("carrier_values"),
        F.sort_array(F.collect_set("carrier")).alias("carrier_list"),
    )
)

result = (
    df_orders.alias("orders")
    .join(order_stats.alias("t"), F.col("orders.id") == F.col("t.order_id"), "inner")
    .select(
        F.col("orders.status"),
        F.col("orders.amount"),
        F.col("t.carrier_values"),
        F.col("t.carrier_list"),
    )
)
