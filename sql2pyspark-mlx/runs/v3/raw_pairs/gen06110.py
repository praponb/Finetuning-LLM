from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy("status")
    .agg(
        F.min("paid_at").alias("min_paid_at"),
        F.avg("amount").alias("avg_amount"),
    )
    .orderBy("avg_amount", "status")
    .limit(20)
)
