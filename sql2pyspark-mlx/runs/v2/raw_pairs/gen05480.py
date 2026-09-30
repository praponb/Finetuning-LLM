from pyspark.sql import functions as F

account_stats = (
    df_transactions
    .filter(F.hour("txn_time").between(12, 20))
    .groupBy("account_id")
    .agg(F.max("txn_time").alias("latest_txn_time"))
)

result = (
    df_accounts.alias("a")
    .join(account_stats.alias("as"), F.col("a.id") == F.col("as.account_id"), "inner")
    .select(F.col("a.id"), F.col("a.status"), F.col("as.latest_txn_time"))
    .orderBy("status")
)
