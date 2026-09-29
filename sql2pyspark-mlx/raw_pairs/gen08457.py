from pyspark.sql import functions as F

result = (
    df_orders.alias("orders")
    .join(df_shipments.alias("shipments"), F.col("shipments.order_id") == F.col("orders.id"), "left")
    .groupBy(F.col("orders.channel"))
    .agg(
        F.count(F.col("shipments.id")).alias("shipment_count"),
        F.max(F.col("shipments.shipped_date")).alias("last_shipped_date"),
    )
    .filter(F.col("shipment_count") > 2)
)
