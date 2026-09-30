from pyspark.sql import functions as F

order_stats = (
    df_shipments
    .groupBy("order_id")
    .agg(
        F.sum("cost").alias("total_cost"),
        F.round(F.avg("cost"), 2).alias("avg_cost"),
    )
)

result = (
    df_orders.alias("o")
    .join(order_stats.alias("os"), F.col("o.id") == F.col("os.order_id"), "inner")
    .select(
        F.col("o.id"),
        F.col("o.channel"),
        F.col("o.shipping_fee"),
        F.col("os.total_cost"),
        F.col("os.avg_cost"),
    )
    .orderBy(F.col("total_cost").desc())
)
