from pyspark.sql import functions as F

result = (
    df_shipments
    .alias("e")
    .join(df_orders.alias("p"), F.col("e.order_id") == F.col("p.id"), "left")
    .groupBy(
        F.col("p.channel").alias("channel"),
        F.col("p.status").alias("order_status"),
        F.col("e.status").alias("shipment_status"),
    )
    .agg(
        F.count("*").alias("rows_total"),
        F.max(F.col("e.cost")).alias("max_cost"),
    )
)
