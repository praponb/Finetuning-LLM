from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("p.order_id"), "left")
    .select(
        F.col("p.status"),
        F.col("p.paid_at"),
        F.col("p.amount"),
        F.col("o.channel"),
        F.col("o.status").alias("order_status"),
        F.col("p.paid_at").cast("date").alias("paid_at_day"),
    )
    .orderBy(F.col("channel").desc())
)
