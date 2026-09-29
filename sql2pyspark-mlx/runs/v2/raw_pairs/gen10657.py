from pyspark.sql import functions as F

account_summary = (
    df_transactions
    .groupBy("account_id")
    .agg(F.max("txn_time").alias("latest_txn_time"))
)

result = (
    df_accounts.alias("a")
    .join(account_summary.alias("t"), F.col("a.id") == F.col("t.account_id"), "left")
    .select(F.col("a.opened_date"), F.col("t.latest_txn_time"))
)
