from pyspark.sql import functions as F

result = (
    df_payments
    .agg(
        F.sum("amount").alias("total_amount"),
        F.min("paid_at").alias("min_paid_at"),
        F.min("amount").alias("min_amount"),
    )
    .orderBy("min_amount", "min_paid_at")
)
