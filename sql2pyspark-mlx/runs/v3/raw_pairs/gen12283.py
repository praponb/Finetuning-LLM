from pyspark.sql import functions as F

account_totals = (
    df_transactions
    .groupBy("account_id")
    .agg(F.count(F.when(F.col("txn_type") == "CREDIT", 1)).alias("credit_count"))
)

result = (
    df_accounts.alias("accounts")
    .join(account_totals.alias("t"), F.col("accounts.id") == F.col("t.account_id"), "inner")
    .select(
        F.col("accounts.id"),
        F.col("accounts.account_type"),
        F.col("accounts.status"),
        F.col("t.credit_count"),
    )
)
