from pyspark.sql import functions as F

order_totals = (
    df_shipments
    .groupBy("order_id")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("status"))).alias("status_values"),
    )
)

result = (
    df_orders.alias("o")
    .join(order_totals.alias("t"), F.col("o.id") == F.col("t.order_id"), "left")
    .select(F.col("o.id"), F.col("o.store_id"), F.col("t.status_values"))
    .orderBy(F.col("store_id").desc())
)
