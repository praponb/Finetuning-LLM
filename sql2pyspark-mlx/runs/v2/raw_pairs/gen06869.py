from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy(F.month("paid_at").alias("paid_month"))
    .agg(
        F.avg("amount").alias("avg_amount"),
        F.sum(F.when(F.col("status") == "SUCCESS", F.col("amount")).otherwise(0)).alias("success_amount"),
        F.count(F.when(F.col("status") == "FAILED", 1)).alias("failed_count"),
        F.count("*").alias("payment_count"),
    )
    .filter(F.col("payment_count") < 1)
    .drop("payment_count")
    .orderBy(F.col("avg_amount").desc(), "paid_month")
    .limit(20)
)
