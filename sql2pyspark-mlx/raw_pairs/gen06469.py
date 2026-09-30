from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "right")
    .select(
        F.col("p.status"),
        F.col("p.amount"),
        F.col("p.paid_at"),
        F.col("o.status").alias("order_status"),
        F.col("o.store_id"),
        F.col("o.amount").alias("order_amount"),
        F.to_date(F.col("p.paid_at")).alias("paid_at_date"),
    )
    .orderBy(F.col("store_id").desc())
)
