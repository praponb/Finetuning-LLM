from pyspark.sql import functions as F

order_totals = (
    df_shipments
    .groupBy("order_id")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("carrier"))).alias("carrier_values"),
        F.max("delivered_date").alias("last_delivered_date"),
    )
)

result = (
    df_orders.alias("o")
    .join(order_totals.alias("ot"), F.col("o.id") == F.col("ot.order_id"), "inner")
    .select(
        F.col("o.id"),
        F.col("o.status"),
        F.col("o.order_date"),
        F.col("ot.carrier_values"),
        F.col("ot.last_delivered_date"),
    )
    .orderBy(F.col("id").desc(), F.col("last_delivered_date").desc())
)
