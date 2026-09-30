from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("account_id").isin([1, 17, 20]))
    .groupBy("is_flagged")
    .agg(
        F.sum(F.when(F.col("txn_type") == "DEBIT", F.col("amount")).otherwise(0)).alias("debit_amount"),
        F.min("txn_time").alias("first_txn_time"),
    )
    .orderBy(F.col("debit_amount").desc(), "first_txn_time", "is_flagged")
    .limit(1)
)
