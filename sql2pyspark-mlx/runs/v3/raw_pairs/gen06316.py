from pyspark.sql import functions as F

result = (
    df_orders.alias("orders")
    .join(df_shipments.alias("shipments"), F.col("orders.id") == F.col("shipments.order_id"), "inner")
    .filter(F.col("shipments.shipped_date") > "2025-08-01")
    .groupBy(F.col("orders.channel"))
    .agg(
        F.count(F.col("shipments.id")).alias("shipment_count"),
        F.concat_ws(", ", F.sort_array(F.collect_set(F.col("shipments.carrier")))).alias("carrier_values"),
    )
    .orderBy("carrier_values", "channel")
    .limit(3)
)
