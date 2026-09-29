from pyspark.sql import functions as F

result = (
    df_payments.alias("payments")
    .join(df_orders.alias("orders"), F.col("payments.order_id") == F.col("orders.id"), "right")
    .groupBy(
        F.date_format(F.col("orders.order_date"), "yyyy-MM").alias("order_month"),
        F.col("orders.channel"),
    )
    .agg(F.max(F.col("payments.paid_at")).alias("last_paid_at"))
    .orderBy("channel", "order_month")
    .limit(20)
)
