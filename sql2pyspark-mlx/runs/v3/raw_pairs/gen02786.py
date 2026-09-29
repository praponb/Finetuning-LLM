from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy("method", F.date_trunc("MONTH", "paid_at").alias("paid_month"))
    .agg(
        F.min("amount").alias("min_amount"),
        F.sum(F.when(F.col("method") == "bank_transfer", F.col("amount")).otherwise(0)).alias("bank_transfer_amount"),
    )
    .filter(F.col("min_amount") < 1040)
    .orderBy("min_amount")
)
