from pyspark.sql import functions as F

account_stats = (
    df_transactions
    .filter(F.col("txn_type").isin(["FEE", "DEBIT", "REFUND"]))
    .groupBy("account_id")
    .agg(F.count(F.when(F.col("txn_type") == "FEE", 1)).alias("fee_count"))
)

result = (
    df_accounts.alias("accounts")
    .join(account_stats.alias("t"), F.col("accounts.id") == F.col("t.account_id"), "left")
    .filter(F.col("t.fee_count") >= 10)
    .select(
        F.col("accounts.id"),
        F.col("accounts.opened_date"),
        F.col("accounts.account_type"),
        F.col("t.fee_count"),
    )
)
