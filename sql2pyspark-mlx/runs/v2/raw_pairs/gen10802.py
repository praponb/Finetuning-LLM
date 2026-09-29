from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_shipments.alias("s"), F.col("o.id") == F.col("s.order_id"), "left")
    .groupBy(F.col("o.channel"))
    .agg(
        F.count(F.col("s.id")).alias("shipment_count"),
        F.sum(F.when(F.col("s.status") == "IN_TRANSIT", F.col("s.cost")).otherwise(0)).alias("in_transit_cost"),
        F.min(F.col("s.cost")).alias("min_cost"),
    )
    .orderBy("shipment_count", F.col("in_transit_cost").desc())
)
