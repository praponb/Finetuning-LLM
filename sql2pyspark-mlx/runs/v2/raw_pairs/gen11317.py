from pyspark.sql import functions as F

result = (
    df_orders.alias("orders")
    .join(df_payments.alias("payments"), F.col("payments.order_id") == F.col("orders.id"), "left")
    .groupBy(F.col("orders.id"), F.col("orders.status"))
    .agg(
        F.count(F.col("payments.id")).alias("payment_count"),
        F.avg(F.col("payments.amount")).alias("avg_amount"),
    )
    .orderBy(F.col("payment_count").desc(), "status")
)
