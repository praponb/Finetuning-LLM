from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_shipments.alias("s"), F.col("o.id") == F.col("s.order_id"), "left")
    .groupBy(F.col("o.id"), F.col("o.status"))
    .agg(
        F.count(F.col("s.id")).alias("shipment_count"),
        F.sum(F.when(F.col("s.carrier") == "FedEx", 1).otherwise(0)).alias("fedex_count"),
        F.round(F.stddev(F.col("s.cost")), 2).alias("stddev_cost"),
    )
)
