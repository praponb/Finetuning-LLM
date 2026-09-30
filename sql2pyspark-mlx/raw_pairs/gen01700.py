from pyspark.sql import functions as F

rollup_totals = (
    df_payments
    .alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "inner")
    .groupBy(
        F.col("o.channel").alias("channel"),
        F.col("o.status").alias("order_status"),
        F.col("s.status").alias("payment_status"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.avg(F.col("s.amount")).alias("avg_amount"),
    )
)

result = rollup_totals.select("channel", "order_status", "payment_status", "avg_amount")
