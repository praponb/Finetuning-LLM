from pyspark.sql import functions as F

account_stats = (
    df_transactions
    .groupBy("account_id")
    .agg(
        F.min("amount").alias("min_amount"),
        F.min("txn_time").alias("min_txn_time"),
    )
)

result = (
    df_accounts.alias("a")
    .join(account_stats.alias("as"), F.col("a.id") == F.col("as.account_id"), "inner")
    .select(
        F.col("a.id"),
        F.col("a.balance"),
        F.col("as.min_amount"),
        F.col("as.min_txn_time"),
    )
)
