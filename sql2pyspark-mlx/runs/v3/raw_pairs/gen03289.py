from pyspark.sql import functions as F

result = (
    df_orders.alias("orders")
    .join(df_payments.alias("payments"), F.col("orders.id") == F.col("payments.order_id"), "inner")
    .groupBy(F.col("orders.id"), F.col("orders.channel"))
    .agg(
        F.count(F.col("payments.id")).alias("payment_count"),
        F.min(F.col("payments.amount")).alias("min_amount"),
        F.avg(F.col("payments.amount")).alias("avg_amount"),
    )
    .filter(F.col("avg_amount") >= 720)
)
