from pyspark.sql import functions as F

result = (
    df_orders.alias("orders")
    .join(df_shipments.alias("shipments"), F.col("orders.id") == F.col("shipments.order_id"), "left_anti")
    .select(F.col("orders.id"), F.col("orders.channel"), F.col("orders.status"))
    .orderBy("channel")
)
