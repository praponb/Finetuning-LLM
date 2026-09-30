from pyspark.sql import functions as F

result = (
    df_payments
    .agg(
        F.round(F.avg("amount"), 2).alias("avg_amount"),
        F.sum(F.when(F.col("method") == "bank_transfer", F.col("amount")).otherwise(0)).alias("bank_transfer_amount"),
    )
    .orderBy("bank_transfer_amount")
)
