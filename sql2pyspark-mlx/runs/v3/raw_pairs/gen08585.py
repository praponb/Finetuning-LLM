from pyspark.sql import functions as F

order_stats = (
    df_shipments
    .filter(F.col("carrier") == "DHL")
    .groupBy("order_id")
    .agg(
        F.max("cost").alias("max_cost"),
        F.count(F.when(F.col("status") == "IN_TRANSIT", 1)).alias("in_transit_count"),
    )
)

result = (
    df_orders.alias("o")
    .join(order_stats.alias("t"), F.col("o.id") == F.col("t.order_id"), "left")
    .select(F.col("o.discount"), F.col("t.max_cost"), F.col("t.in_transit_count"))
    .orderBy(F.col("discount").desc(), "in_transit_count")
)
