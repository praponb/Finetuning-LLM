from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_shipments.alias("s"), F.col("s.order_id") == F.col("o.id"), "left")
    .filter(F.col("s.carrier") == "UPS")
    .groupBy(F.col("o.id"), F.col("o.channel"))
    .agg(
        F.count(F.col("s.id")).alias("shipment_count"),
        F.min(F.col("s.shipped_date")).alias("min_shipped_date"),
    )
    .filter(F.col("shipment_count") > 3)
)
