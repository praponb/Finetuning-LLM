from pyspark.sql import functions as F

order_summary = (
    df_shipments
    .groupBy("order_id")
    .agg(F.round(F.avg("cost"), 2).alias("avg_cost"))
)

result = (
    df_orders.alias("o")
    .join(order_summary.alias("os"), F.col("o.id") == F.col("os.order_id"), "left")
    .select(F.col("o.id"), F.col("o.discount"), F.col("os.avg_cost"))
    .orderBy(F.col("avg_cost").desc())
)
