from pyspark.sql import functions as F

result = (
    df_payments
    .filter((F.to_date("paid_at") < "2025-09-20") & F.col("amount").between(850, 920))
    .groupBy("method")
    .agg(
        F.min("paid_at").alias("first_paid_at"),
        F.sum(F.when(F.col("method") == "bank_transfer", F.col("amount")).otherwise(0)).alias("bank_transfer_amount"),
        F.sum(F.when(F.col("method") == "cash", F.col("amount")).otherwise(0)).alias("cash_amount"),
    )
)
