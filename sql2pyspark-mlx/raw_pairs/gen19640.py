from pyspark.sql import functions as F

order_totals = (
    df_shipments
    .filter(F.col("order_id").isNull())
    .groupBy("order_id")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("carrier"))).alias("carrier_values"),
        F.round(((100 * F.sum(F.when(F.col("status") == "LOST", 1).otherwise(0))) / F.count("*")), 1).alias("pct_lost"),
    )
)

result = (
    df_orders.alias("o")
    .join(order_totals.alias("O"), F.col("o.id") == F.col("O.order_id"), "left")
    .select(
        F.col("o.id"),
        F.col("o.shipping_fee"),
        F.col("O.carrier_values"),
        F.col("O.pct_lost"),
    )
    .orderBy(F.col("carrier_values").desc())
)
