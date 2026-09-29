from pyspark.sql import functions as F

account_stats = (
    df_transactions
    .filter(F.col("txn_time") >= "2025-08-01")
    .groupBy("account_id")
    .agg(F.round(F.avg("amount"), 2).alias("avg_amount"))
)

result = (
    df_accounts.alias("a")
    .join(account_stats.alias("t"), F.col("a.id") == F.col("t.account_id"), "inner")
    .select(F.col("a.id"), F.col("a.balance"), F.col("t.avg_amount"))
)
