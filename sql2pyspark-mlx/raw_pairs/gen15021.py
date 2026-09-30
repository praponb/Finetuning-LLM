from pyspark.sql import functions as F

account_summary = (
    df_transactions
    .filter(F.col("is_flagged"))
    .groupBy("account_id")
    .agg(
        F.round(F.stddev("amount"), 2).alias("stddev_amount"),
        F.min("txn_time").alias("min_txn_time"),
    )
)

result = (
    df_accounts.alias("a")
    .join(account_summary.alias("as"), F.col("a.id") == F.col("as.account_id"), "left")
    .filter(F.col("as.stddev_amount") > 80)
    .select(
        F.col("a.id"),
        F.col("a.balance"),
        F.col("as.stddev_amount"),
        F.col("as.min_txn_time"),
    )
    .orderBy("id", F.col("stddev_amount").desc())
)
