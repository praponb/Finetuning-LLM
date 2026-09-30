from pyspark.sql import functions as F

order_stats = (
    df_shipments
    .groupBy("order_id")
    .agg(
        F.min("cost").alias("min_cost"),
        F.count("cost").alias("cost_count"),
    )
)

result = (
    df_orders.alias("o")
    .join(order_stats.alias("t"), F.col("o.id") == F.col("t.order_id"), "left")
    .filter(F.col("t.min_cost") >= 36)
    .select(
        F.col("o.channel"),
        F.col("o.customer_id"),
        F.col("t.min_cost"),
        F.col("t.cost_count"),
    )
)
