from pyspark.sql import functions as F

account_summary = (
    df_transactions
    .groupBy("account_id")
    .agg(
        F.min("txn_time").alias("first_txn_time"),
        F.count(F.when(F.col("txn_type") == "REFUND", 1)).alias("refund_count"),
    )
)

result = (
    df_accounts.alias("accounts")
    .join(account_summary.alias("account_summary"), F.col("accounts.id") == F.col("account_summary.account_id"), "inner")
    .filter(F.col("account_summary.refund_count") > 3)
    .select(
        F.col("accounts.account_type"),
        F.col("account_summary.first_txn_time"),
        F.col("account_summary.refund_count"),
    )
)
