from pyspark.sql import functions as F

order_summary = (
    df_shipments
    .groupBy("order_id")
    .agg(F.round(F.avg("cost"), 2).alias("avg_cost"))
)

result = (
    df_orders.alias("o")
    .join(order_summary.alias("t"), F.col("o.id") == F.col("t.order_id"), "left")
    .filter(F.col("t.avg_cost") > 78)
    .select(F.col("o.amount"), F.col("o.channel"), F.col("t.avg_cost"))
    .orderBy("channel", F.col("amount").desc())
)
