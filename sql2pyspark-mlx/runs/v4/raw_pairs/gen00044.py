from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy("status", "method")
    .agg(
        F.min("paid_at").alias("min_paid_at"),
        F.sum(F.when(F.col("status") == "FAILED", 1).otherwise(0)).alias("failed_count"),
    )
)
