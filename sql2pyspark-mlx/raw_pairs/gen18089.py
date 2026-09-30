from pyspark.sql import functions as F

order_totals = (
    df_shipments
    .filter(F.col("carrier") != "DHL")
    .groupBy("order_id")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("status"))).alias("status_values"),
    )
)

result = (
    df_orders.alias("o")
    .join(order_totals.alias("ot"), F.col("o.id") == F.col("ot.order_id"), "left")
    .select(
        F.col("o.id"),
        F.col("o.channel"),
        F.col("o.shipping_fee"),
        F.col("ot.status_values"),
    )
)
