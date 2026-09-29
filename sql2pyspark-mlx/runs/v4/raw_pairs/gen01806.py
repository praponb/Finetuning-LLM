from pyspark.sql import functions as F

account_summary = (
    df_transactions
    .filter(F.col("is_flagged"))
    .groupBy("account_id")
    .agg(
        F.count(F.when(F.col("txn_type") == "REFUND", 1)).alias("refund_count"),
        F.min("amount").alias("min_amount"),
    )
)

result = (
    df_accounts.alias("accounts")
    .join(account_summary.alias("account_summary"), F.col("accounts.id") == F.col("account_summary.account_id"), "inner")
    .select(
        F.col("accounts.id"),
        F.col("accounts.opened_date"),
        F.col("account_summary.refund_count"),
        F.col("account_summary.min_amount"),
    )
    .orderBy("opened_date")
)
