from pyspark.sql import functions as F

order_stats = (
    df_shipments
    .groupBy("order_id")
    .agg(F.count(F.when(F.col("status") == "IN_TRANSIT", 1)).alias("in_transit_count"))
)

result = (
    df_orders.alias("o")
    .join(order_stats.alias("t"), F.col("o.id") == F.col("t.order_id"), "inner")
    .filter(F.col("t.in_transit_count") > 8)
    .select(F.col("o.customer_id"), F.col("o.channel"), F.col("t.in_transit_count"))
    .orderBy(F.col("channel").desc())
)
