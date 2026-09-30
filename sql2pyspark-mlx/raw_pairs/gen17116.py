from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_shipments.alias("s"), F.col("o.id") == F.col("s.order_id"), "left")
    .filter(F.col("s.order_id").isNotNull())
    .groupBy(F.col("o.id"), F.col("o.channel"))
    .agg(
        F.count(F.col("s.id")).alias("shipment_count"),
        F.count(F.col("s.cost")).alias("cost_count"),
    )
)
