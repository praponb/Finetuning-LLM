from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("merchant") == "Amazon")
    .groupBy("txn_type", "account_id")
    .agg(
        F.sum(F.when(F.col("txn_type") == "REFUND", 1).otherwise(0)).alias("refund_count"),
    )
    .orderBy(F.col("txn_type").desc(), F.col("account_id").desc())
)
