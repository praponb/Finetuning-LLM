from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_shipments.alias("s"), F.col("s.order_id") == F.col("o.id"), "left")
    .groupBy(F.col("o.id"))
    .agg(
        F.count(F.col("s.id")).alias("shipment_count"),
        F.count(F.col("s.cost")).alias("cost_count"),
        F.concat_ws(", ", F.sort_array(F.collect_set(F.col("s.carrier")))).alias("carrier_values"),
    )
    .orderBy(F.col("shipment_count").desc(), F.col("id").desc())
)
