from pyspark.sql import functions as F

account_summary = (
    df_transactions
    .filter(F.col("amount") > 2000)
    .groupBy("account_id")
    .agg(F.min("txn_time").alias("first_txn_time"))
)

result = (
    df_accounts.alias("accounts")
    .join(account_summary.alias("t"), F.col("accounts.id") == F.col("t.account_id"), "inner")
    .select(
        F.col("accounts.id"),
        F.col("accounts.balance"),
        F.col("accounts.opened_date"),
        F.col("t.first_txn_time"),
    )
    .orderBy("id")
)
