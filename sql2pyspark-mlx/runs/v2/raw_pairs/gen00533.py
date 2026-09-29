from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_shipments.alias("s"), F.col("s.order_id") == F.col("o.id"), "inner")
    .groupBy(F.col("o.status"))
    .agg(
        F.count(F.col("s.id")).alias("shipment_count"),
        F.sum(F.when(F.col("s.carrier") == "Flash", 1).otherwise(0)).alias("flash_count"),
    )
)
