from pyspark.sql import functions as F

result = (
    df_payments.alias("payments")
    .join(df_orders.alias("orders"), F.col("payments.order_id") == F.col("orders.id"), "inner")
    .groupBy(F.col("orders.channel"))
    .agg(
        F.max(F.col("payments.paid_at")).alias("last_paid_at"),
        F.round(F.avg(F.col("payments.amount")), 2).alias("avg_amount"),
    )
    .filter(F.col("avg_amount") > 39)
)
