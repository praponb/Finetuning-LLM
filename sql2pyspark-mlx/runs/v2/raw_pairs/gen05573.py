from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("method").isin(["bank_transfer", "paypal", "card"]) & (F.col("paid_at") >= "2025-03-13"))
    .groupBy("status", F.quarter("paid_at").alias("paid_quarter"))
    .agg(
        F.sum(F.when(F.col("method") == "bank_transfer", F.col("amount")).otherwise(0)).alias("bank_transfer_amount"),
        F.avg("amount").alias("avg_amount"),
    )
    .filter(F.col("bank_transfer_amount") < 33)
)
