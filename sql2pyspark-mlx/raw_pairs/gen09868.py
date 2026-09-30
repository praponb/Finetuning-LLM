from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_shipments.alias("s"), F.col("o.id") == F.col("s.order_id"), "inner")
    .filter(F.col("o.amount") >= 1410)
    .groupBy(F.col("o.id"), F.col("o.channel"))
    .agg(
        F.count(F.col("s.id")).alias("shipment_count"),
        F.sum(F.when(F.col("s.carrier") == "UPS", 1).otherwise(0)).alias("ups_count"),
    )
)
