from pyspark.sql import functions as F

shipments_by_order = (
    df_shipments
    .groupBy("order_id")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("carrier"))).alias("carrier_values"),
    )
)

result = (
    df_orders.alias("orders")
    .join(shipments_by_order.alias("shipments_by_order"), F.col("orders.id") == F.col("shipments_by_order.order_id"), "left")
    .select(F.col("orders.channel"), F.col("shipments_by_order.carrier_values"))
)
