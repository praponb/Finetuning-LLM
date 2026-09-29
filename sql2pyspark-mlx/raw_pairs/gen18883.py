from pyspark.sql import functions as F

result = (
    df_shipments
    .alias("s")
    .join(df_orders.alias("d"), F.col("s.order_id") == F.col("d.id"), "inner")
    .groupingSets(
        [["channel", "order_status", "shipment_status"], ["channel"]],
        F.col("d.channel").alias("channel"),
        F.col("d.status").alias("order_status"),
        F.col("s.status").alias("shipment_status"),
    )
    .agg(
        F.count("*").alias("rows_total"),
        F.max(F.col("s.cost")).alias("max_cost"),
    )
)
