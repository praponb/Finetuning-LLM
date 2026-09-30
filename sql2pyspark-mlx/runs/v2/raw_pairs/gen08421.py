from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy("status")
    .agg(
        F.sum(F.when(F.col("method") == "bank_transfer", 1).otherwise(0)).alias("bank_transfer_count"),
        F.sum("amount").alias("total_amount"),
    )
    .filter(F.col("total_amount") < 8600)
)
