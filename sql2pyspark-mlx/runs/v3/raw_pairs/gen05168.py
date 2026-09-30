from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("method").isNotNull())
    .groupBy("status")
    .agg(
        F.min("amount").alias("min_amount"),
        F.max("paid_at").alias("last_paid_at"),
        F.count("*").alias("total_count"),
    )
)
