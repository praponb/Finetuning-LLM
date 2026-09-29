from pyspark.sql import functions as F

account_stats = (
    df_transactions
    .groupBy("account_id")
    .agg(F.min("txn_time").alias("first_txn_time"))
)

result = (
    df_accounts.alias("a")
    .join(account_stats.alias("as"), F.col("a.id") == F.col("as.account_id"), "inner")
    .select(F.col("a.balance"), F.col("as.first_txn_time"))
    .orderBy(F.col("first_txn_time").desc())
)
