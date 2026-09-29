from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_shipments.alias("s"), F.col("o.id") == F.col("s.order_id"), "left")
    .filter(F.col("s.status") == "LOST")
    .groupBy(F.col("o.id"), F.col("o.status"))
    .agg(
        F.count(F.col("s.id")).alias("shipment_count"),
        F.max(F.col("s.shipped_date")).alias("last_shipped_date"),
        F.round(F.stddev(F.col("s.cost")), 2).alias("stddev_cost"),
    )
)
