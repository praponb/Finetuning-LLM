from pyspark.sql import functions as F

shipments_by_order = (
    df_shipments
    .groupBy("order_id")
    .agg(
        (F.max("cost") - F.min("cost")).alias("cost_range"),
        F.concat_ws(", ", F.sort_array(F.collect_set("status"))).alias("status_values"),
    )
)

result = (
    df_orders.alias("o")
    .join(shipments_by_order.alias("t"), F.col("o.id") == F.col("t.order_id"), "inner")
    .select(
        F.col("o.channel"),
        F.col("o.order_date"),
        F.col("t.cost_range"),
        F.col("t.status_values"),
    )
    .orderBy(F.col("status_values").desc(), F.col("cost_range").desc())
)
