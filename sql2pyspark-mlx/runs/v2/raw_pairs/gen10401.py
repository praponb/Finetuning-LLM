from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("amount").between(1250, 1600))
    .groupBy("status", F.month("paid_at").alias("paid_month"))
    .agg(
        F.sum(F.when(F.col("method") == "promptpay", 1).otherwise(0)).alias("promptpay_count"),
        F.sum(F.when(F.col("method") == "bank_transfer", F.col("amount")).otherwise(0)).alias("bank_transfer_amount"),
        F.count(F.when(F.col("status") == "SUCCESS", 1)).alias("success_count"),
        F.min("amount").alias("min_amount"),
    )
    .filter(F.col("min_amount") > 690)
    .drop("min_amount")
)
