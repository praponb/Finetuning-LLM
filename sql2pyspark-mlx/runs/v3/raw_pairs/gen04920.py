from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_shipments.alias("s"), F.col("o.id") == F.col("s.order_id"), "left")
    .filter(F.col("o.discount") < 0.36)
    .groupBy(F.col("o.id"))
    .agg(
        F.count(F.col("s.id")).alias("shipment_count"),
        F.min(F.col("s.cost")).alias("min_cost"),
        F.round(((100 * F.sum(F.when(F.col("s.carrier") == "FedEx", 1).otherwise(0))) / F.count("*")), 1).alias("pct_fedex"),
    )
    .filter(F.col("shipment_count") >= 1)
)
