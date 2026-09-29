from pyspark.sql import functions as F

account_totals = (
    df_transactions
    .filter(F.col("txn_type") != "REFUND")
    .groupBy("account_id")
    .agg(F.max("txn_time").alias("latest_txn_time"))
)

result = (
    df_accounts.alias("accounts")
    .join(account_totals.alias("t"), F.col("accounts.id") == F.col("t.account_id"), "inner")
    .select(
        F.col("accounts.id"),
        F.col("accounts.opened_date"),
        F.col("t.latest_txn_time"),
    )
    .orderBy("id")
    .limit(5)
)
