from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("txn_type") != "FEE")
    .groupBy("merchant", F.date_format("txn_time", "yyyy-MM").alias("txn_month"))
    .agg(
        F.sum(F.when(F.col("txn_type") == "REFUND", 1).otherwise(0)).alias("refund_count"),
    )
    .filter(F.col("refund_count") >= 26)
)
