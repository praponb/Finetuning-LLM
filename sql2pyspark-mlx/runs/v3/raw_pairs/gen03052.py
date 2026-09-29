from pyspark.sql import functions as F

result = (
    df_shipments.alias("shipments")
    .join(df_orders.alias("orders"), F.col("shipments.order_id") == F.col("orders.id"), "left")
    .filter(F.col("shipments.cost") > 53)
    .groupBy(F.col("orders.channel"), F.col("orders.status"))
    .agg(F.avg(F.col("shipments.cost")).alias("avg_cost"))
    .orderBy(F.col("status").desc())
)
