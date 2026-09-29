from pyspark.sql import functions as F

account_summary = (
    df_transactions
    .filter(F.hour("txn_time").between(8, 12))
    .groupBy("account_id")
    .agg(F.min("txn_time").alias("min_txn_time"))
)

result = (
    df_accounts.alias("a")
    .join(account_summary.alias("as"), F.col("a.id") == F.col("as.account_id"), "left")
    .select(F.col("a.balance"), F.col("a.customer_id"), F.col("as.min_txn_time"))
    .orderBy("min_txn_time")
)
