from pyspark.sql import functions as F

account_stats = (
    df_transactions
    .filter(F.col("txn_time") >= "2024-05-08")
    .groupBy("account_id")
    .agg(F.max("txn_time").alias("last_txn_time"))
)

result = (
    df_accounts.alias("a")
    .join(account_stats.alias("as"), F.col("a.id") == F.col("as.account_id"), "left")
    .select(F.col("a.opened_date"), F.col("a.account_type"), F.col("as.last_txn_time"))
)
