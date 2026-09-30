from pyspark.sql import functions as F

filtered_payments = (
    df_payments
    .filter((F.col("paid_at") >= "2024-05-01") & F.col("amount").between(480, 1580))
)

summary = (
    filtered_payments
    .groupBy("order_id", "status")
    .agg(
        F.sum(F.when(F.col("method") == "bank_transfer", F.col("amount")).otherwise(0)).alias("bank_transfer_amount"),
        F.round(F.avg("amount"), 2).alias("avg_amount"),
    )
)

result = (
    summary
    .filter(F.col("avg_amount") >= 76)
    .orderBy("bank_transfer_amount", "order_id", "status")
    .limit(10)
)
