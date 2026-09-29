from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("paid_at") >= "2025-02-25")
    .groupBy("method", "status")
    .agg(
        F.count("amount").alias("amount_count"),
        F.count(F.when(F.col("method") == "promptpay", 1)).alias("promptpay_count"),
    )
    .orderBy("method")
)
