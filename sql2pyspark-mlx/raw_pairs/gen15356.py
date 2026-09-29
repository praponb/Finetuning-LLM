from pyspark.sql import functions as F

account_stats = (
    df_transactions
    .filter(F.col("amount") > 3600)
    .groupBy("account_id")
    .agg(
        F.min("txn_time").alias("min_txn_time"),
        (F.max("amount") - F.min("amount")).alias("amount_range"),
    )
)

result = (
    df_accounts.alias("a")
    .join(account_stats.alias("t"), F.col("a.id") == F.col("t.account_id"), "left")
    .filter(F.col("t.amount_range") >= 34)
    .select(
        F.col("a.id"),
        F.col("a.balance"),
        F.col("a.status"),
        F.col("t.min_txn_time"),
        F.col("t.amount_range"),
    )
)
