from pyspark.sql import functions as F

shipments_by_order = (
    df_shipments
    .filter(F.col("carrier").isin(["FedEx", "DHL", "Flash"]))
    .groupBy("order_id")
    .agg(F.count(F.when(F.col("carrier") == "DHL", 1)).alias("dhl_count"))
)

result = (
    df_orders.alias("orders")
    .join(shipments_by_order.alias("shipments_by_order"), F.col("orders.id") == F.col("shipments_by_order.order_id"), "left")
    .select(
        F.col("orders.id"),
        F.col("orders.order_date"),
        F.col("shipments_by_order.dhl_count"),
    )
    .orderBy(F.col("dhl_count").desc())
)
