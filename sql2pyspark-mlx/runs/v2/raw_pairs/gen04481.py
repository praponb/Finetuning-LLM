from pyspark.sql import functions as F

account_stats = (
    df_transactions
    .groupBy("account_id")
    .agg(F.min("txn_time").alias("first_txn_time"))
)

result = (
    df_accounts.alias("accounts")
    .join(account_stats.alias("t"), F.col("accounts.id") == F.col("t.account_id"), "left")
    .select(F.col("accounts.opened_date"), F.col("t.first_txn_time"))
    .orderBy(F.col("opened_date").desc())
)
