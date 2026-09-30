from pyspark.sql import functions as F

result = (
    df_orders.alias("orders")
    .join(df_payments.alias("payments"), F.col("payments.order_id") == F.col("orders.id"), "left")
    .groupBy(F.col("orders.id"), F.col("orders.channel"))
    .agg(
        F.count(F.col("payments.id")).alias("payment_count"),
        (F.max(F.col("payments.amount")) - F.min(F.col("payments.amount"))).alias("amount_range"),
        F.round(F.avg(F.col("payments.amount")), 2).alias("avg_amount"),
    )
    .orderBy(F.col("avg_amount").desc())
)
