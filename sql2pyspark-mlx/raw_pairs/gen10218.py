from pyspark.sql import functions as F

shipments_by_order = (
    df_shipments
    .filter(F.col("cost") >= 46)
    .groupBy("order_id")
    .agg(
        F.count("*").alias("shipment_count"),
        F.count("*").alias("cnt"),
    )
)

result = (
    df_orders.alias("orders")
    .join(shipments_by_order.alias("t"), F.col("orders.id") == F.col("t.order_id"), "inner")
    .select(
        F.col("orders.discount"),
        F.col("orders.channel"),
        F.col("t.shipment_count"),
        F.col("t.cnt"),
    )
)
