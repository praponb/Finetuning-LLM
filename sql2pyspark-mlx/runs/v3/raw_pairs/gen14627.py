from pyspark.sql import functions as F

result = (
    df_payments.alias("payments")
    .join(df_orders.alias("orders"), F.col("payments.order_id") == F.col("orders.id"), "left")
    .filter(F.col("orders.order_date") < "2025-09-26")
    .groupBy(
        F.when(F.col("orders.channel").isin(["mobile", "web"]), "group_a").otherwise("other").alias("channel_group"),
    )
    .agg(
        F.avg(F.col("payments.amount")).alias("avg_amount"),
        F.max(F.col("payments.paid_at")).alias("last_paid_at"),
    )
)
