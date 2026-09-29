from pyspark.sql import functions as F

result = (
    df_payments
    .filter((F.col("status") == "FAILED") | (F.col("status") == "REFUNDED"))
    .cube("method", "status")
    .agg(
        F.max("amount").alias("max_amount"),
        F.sum(F.when(F.col("method") == "bank_transfer", F.col("amount")).otherwise(0)).alias("bank_transfer_amount"),
    )
)
