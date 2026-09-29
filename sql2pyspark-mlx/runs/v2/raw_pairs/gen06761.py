from pyspark.sql import functions as F

result = (
    df_orders.alias("orders")
    .join(df_shipments.alias("shipments"), F.col("shipments.order_id") == F.col("orders.id"), "inner")
    .filter(~F.col("shipments.status").isin(["IN_TRANSIT", "DELIVERED", "LOST"]))
    .groupBy(F.col("orders.id"), F.col("orders.status"))
    .agg(F.count(F.col("shipments.id")).alias("shipment_count"))
)
