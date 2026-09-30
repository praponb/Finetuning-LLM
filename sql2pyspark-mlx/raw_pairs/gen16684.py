from pyspark.sql import functions as F

result = (
    df_payments
    .alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "left")
    .rollup(
        F.col("o.status").alias("order_status"),
        F.col("s.status").alias("payment_status"),
    )
    .agg(
        F.count("*").alias("rows_total"),
        F.avg(F.col("s.amount")).alias("avg_amount"),
    )
)
