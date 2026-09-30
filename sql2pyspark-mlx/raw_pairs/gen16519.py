from pyspark.sql import functions as F

account_stats = (
    df_transactions
    .filter(F.col("amount") <= 3800)
    .groupBy("account_id")
    .agg(
        F.max("txn_time").alias("latest_txn_time"),
        (F.max("amount") - F.min("amount")).alias("amount_range"),
    )
)

result = (
    df_accounts.alias("a")
    .join(account_stats.alias("t"), F.col("a.id") == F.col("t.account_id"), "left")
    .select(
        F.col("a.balance"),
        F.col("a.opened_date"),
        F.col("t.latest_txn_time"),
        F.col("t.amount_range"),
    )
    .orderBy(F.col("balance").desc())
)
