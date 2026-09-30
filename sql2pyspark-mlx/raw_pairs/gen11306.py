from pyspark.sql import functions as F

result = (
    df_orders.alias("orders")
    .join(df_shipments.alias("shipments"), F.col("shipments.order_id") == F.col("orders.id"), "left")
    .filter(F.col("orders.status").isin(["CANCELLED", "SHIPPED"]))
    .groupBy(F.col("orders.channel"))
    .agg(
        F.count(F.col("shipments.id")).alias("shipment_count"),
        F.concat_ws(", ", F.sort_array(F.collect_set(F.col("shipments.status")))).alias("status_values"),
        F.count(F.when(F.col("shipments.status") == "LOST", 1)).alias("lost_count"),
    )
    .orderBy(F.col("lost_count").desc(), "status_values")
)
