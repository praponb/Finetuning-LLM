from pyspark.sql import functions as F

result = (
    df_shipments.alias("shipments")
    .join(df_orders.alias("orders"), F.col("shipments.order_id") == F.col("orders.id"), "inner")
    .groupBy(F.col("orders.channel"))
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set(F.col("shipments.status")))).alias("status_values"),
        F.sum(F.when(F.col("shipments.status") == "IN_TRANSIT", 1).otherwise(0)).alias("in_transit_count"),
    )
    .orderBy(F.col("status_values").desc(), "in_transit_count")
)
