from pyspark.sql import functions as F

result = (
    df_shipments.alias("shipments")
    .join(df_orders.alias("orders"), F.col("shipments.order_id") == F.col("orders.id"), "outer")
    .groupBy(F.col("orders.status"), F.col("orders.channel"))
    .agg(F.min(F.col("shipments.delivered_date")).alias("first_delivered_date"))
    .orderBy(F.col("status").desc(), "first_delivered_date")
)
