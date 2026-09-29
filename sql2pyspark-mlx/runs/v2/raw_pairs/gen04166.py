from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_shipments.alias("s"), F.col("o.id") == F.col("s.order_id"), "left")
    .groupBy(F.col("o.channel"))
    .agg(
        F.count(F.col("s.id")).alias("shipment_count"),
        F.max(F.col("s.delivered_date")).alias("latest_delivered_date"),
    )
    .orderBy("channel", "latest_delivered_date")
    .limit(10)
)
