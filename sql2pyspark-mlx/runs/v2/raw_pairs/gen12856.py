from pyspark.sql import functions as F

order_stats = (
    df_payments
    .groupBy("order_id")
    .agg(
        F.round(F.avg("amount"), 2).alias("avg_amount"),
        F.count(F.when(F.col("method") == "cash", 1)).alias("cash_count"),
    )
)

result = (
    df_orders.alias("o")
    .join(order_stats.alias("O"), F.col("o.id") == F.col("O.order_id"), "left")
    .filter(F.col("O.cash_count") >= 4)
    .select(
        F.col("o.id"),
        F.col("o.order_date"),
        F.col("o.channel"),
        F.col("O.avg_amount"),
        F.col("O.cash_count"),
    )
)
