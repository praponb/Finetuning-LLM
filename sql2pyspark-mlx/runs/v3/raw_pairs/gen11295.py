from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy("status")
    .agg(
        F.max("paid_at").alias("latest_paid_at"),
        F.max("amount").alias("max_amount"),
    )
)
