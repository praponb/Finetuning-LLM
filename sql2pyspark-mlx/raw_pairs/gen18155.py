from pyspark.sql import functions as F

account_stats = (
    df_transactions
    .groupBy("account_id")
    .agg(F.min("txn_time").alias("min_txn_time"))
)

result = (
    df_accounts.alias("a")
    .join(account_stats.alias("t"), F.col("a.id") == F.col("t.account_id"), "inner")
    .select(F.col("a.account_type"), F.col("a.opened_date"), F.col("t.min_txn_time"))
    .orderBy("account_type", "opened_date")
)
