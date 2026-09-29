from pyspark.sql import functions as F

result = (
    df_orders.alias("orders")
    .join(df_shipments.alias("shipments"), F.col("orders.id") == F.col("shipments.order_id"), "left")
    .groupBy(F.col("orders.id"))
    .agg(
        F.count(F.col("shipments.id")).alias("shipment_count"),
        F.count(F.when(F.col("shipments.carrier") == "DHL", 1)).alias("dhl_count"),
    )
    .filter(F.col("dhl_count") >= 3)
    .orderBy(F.col("dhl_count").desc(), F.col("shipment_count").desc())
)
