from pyspark.sql import functions as F

account_summary = (
    df_transactions
    .groupBy("account_id")
    .agg(
        F.min("txn_time").alias("min_txn_time"),
        F.min("amount").alias("min_amount"),
    )
)

result = (
    df_accounts.alias("a")
    .join(account_summary.alias("as"), F.col("a.id") == F.col("as.account_id"), "left")
    .filter(F.col("as.min_amount") > 3200)
    .select(F.col("a.balance"), F.col("as.min_txn_time"), F.col("as.min_amount"))
    .orderBy("min_txn_time")
)
