from pyspark.sql import functions as F

result = (
    df_orders.alias("orders")
    .join(df_shipments.alias("shipments"), F.col("orders.id") == F.col("shipments.order_id"), "left")
    .filter(F.col("orders.shipping_fee") <= 22)
    .groupBy(F.col("orders.id"), F.col("orders.status"))
    .agg(
        F.count(F.col("shipments.id")).alias("shipment_count"),
        F.min(F.col("shipments.shipped_date")).alias("min_shipped_date"),
        F.sum(F.when(F.col("shipments.carrier") == "FedEx", F.col("shipments.cost")).otherwise(0)).alias("fedex_cost"),
    )
    .orderBy("min_shipped_date")
)
