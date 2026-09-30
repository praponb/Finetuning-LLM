from pyspark.sql import functions as F

shipments_by_order = (
    df_shipments
    .filter(F.col("carrier") != "FedEx")
    .groupBy("order_id")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("carrier"))).alias("carrier_values"),
    )
)

result = (
    df_orders.alias("o")
    .join(shipments_by_order.alias("sbo"), F.col("o.id") == F.col("sbo.order_id"), "left")
    .select(F.col("o.store_id"), F.col("o.discount"), F.col("sbo.carrier_values"))
)
