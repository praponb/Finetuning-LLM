from pyspark.sql import functions as F

recent_orders = df_orders.filter(F.col("status").isin(["RETURNED", "CANCELLED", "COMPLETED"]))

orders_stats = (
    recent_orders
    .groupBy("channel")
    .agg(
        F.round(F.stddev("discount"), 2).alias("stddev_discount"),
        F.round(F.avg("amount"), 2).alias("avg_amount"),
    )
)

result = (
    orders_stats
    .filter(F.col("avg_amount") > 29)
    .select("channel", "stddev_discount", "avg_amount")
    .orderBy("avg_amount")
)
