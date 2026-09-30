from pyspark.sql import functions as F

result = (
    df_orders.alias("orders")
    .join(df_shipments.alias("shipments"), F.col("shipments.order_id") == F.col("orders.id"), "left")
    .filter(F.col("orders.amount").isNull())
    .groupBy(F.col("orders.id"))
    .agg(F.count(F.col("shipments.id")).alias("shipment_count"))
)
