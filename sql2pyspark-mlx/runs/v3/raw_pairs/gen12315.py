from pyspark.sql import functions as F

account_summary = (
    df_transactions
    .filter(F.col("amount") <= 3600)
    .groupBy("account_id")
    .agg(
        F.max("txn_time").alias("latest_txn_time"),
        F.sum("amount").alias("total_amount"),
    )
)

result = (
    df_accounts.alias("a")
    .join(account_summary.alias("as"), F.col("a.id") == F.col("as.account_id"), "left")
    .filter(F.col("as.total_amount") >= 21000)
    .select(
        F.col("a.id"),
        F.col("a.customer_id"),
        F.col("as.latest_txn_time"),
        F.col("as.total_amount"),
    )
)
