from pyspark.sql import functions as F

rollup_totals = (
    df_payments
    .alias("a")
    .join(df_orders.alias("d"), F.col("a.order_id") == F.col("d.id"), "inner")
    .rollup(
        F.col("d.channel").alias("channel"),
        F.col("d.status").alias("order_status"),
        F.col("a.method").alias("method"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.round(F.sum(F.col("a.amount")), 2).alias("total_amount"),
    )
)

result = (
    rollup_totals
    .select("method", "cnt")
    .orderBy("method", "cnt")
)
