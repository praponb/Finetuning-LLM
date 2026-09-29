from pyspark.sql import functions as F

account_totals = (
    df_transactions
    .groupBy("account_id")
    .agg(F.max("txn_time").alias("last_txn_time"))
)

result = (
    df_accounts.alias("accounts")
    .join(account_totals.alias("t"), F.col("accounts.id") == F.col("t.account_id"), "inner")
    .select(
        F.col("accounts.id"),
        F.col("accounts.status"),
        F.col("accounts.balance"),
        F.col("t.last_txn_time"),
    )
    .orderBy(F.col("last_txn_time").desc())
)
