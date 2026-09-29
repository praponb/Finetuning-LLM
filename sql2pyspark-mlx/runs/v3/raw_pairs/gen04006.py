from pyspark.sql import functions as F

order_stats = (
    df_shipments
    .groupBy("order_id")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("carrier") == "Flash", 1).otherwise(0))) / F.count("*")), 1).alias("pct_flash"),
        F.concat_ws(", ", F.sort_array(F.collect_set("carrier"))).alias("carrier_values"),
    )
)

result = (
    df_orders.alias("orders")
    .join(order_stats.alias("order_stats"), F.col("orders.id") == F.col("order_stats.order_id"), "inner")
    .filter(F.col("order_stats.pct_flash") > 58)
    .select(
        F.col("orders.amount"),
        F.col("orders.order_date"),
        F.col("order_stats.pct_flash"),
        F.col("order_stats.carrier_values"),
    )
    .orderBy("amount")
)
