from pyspark.sql import functions as F

recent_payments = df_payments.filter(F.col("amount").between(610, 920))

payments_stats = (
    recent_payments
    .groupBy("status")
    .agg(
        F.max("paid_at").alias("last_paid_at"),
        F.avg("amount").alias("avg_amount"),
    )
)

result = payments_stats
