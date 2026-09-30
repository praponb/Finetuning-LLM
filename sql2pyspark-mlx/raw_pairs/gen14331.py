from pyspark.sql import functions as F

order_summary = (
    df_shipments
    .groupBy("order_id")
    .agg(
        F.count("cost").alias("cost_count"),
        F.round(F.avg("cost"), 2).alias("avg_cost"),
    )
)

result = (
    df_orders.alias("o")
    .join(order_summary.alias("t"), F.col("o.id") == F.col("t.order_id"), "inner")
    .select(
        F.col("o.discount"),
        F.col("o.status"),
        F.col("t.cost_count"),
        F.col("t.avg_cost"),
    )
)
