from pyspark.sql import functions as F

result = (
    df_payments
    .agg(
        F.max("paid_at").alias("latest_paid_at"),
        F.sum(F.when(F.col("status") == "FAILED", F.col("amount")).otherwise(0)).alias("failed_amount"),
    )
    .orderBy("latest_paid_at")
)
