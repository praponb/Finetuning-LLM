from pyspark.sql import functions as F

transactions_by_account = (
    df_transactions
    .groupBy("account_id")
    .agg(
        F.max("txn_time").alias("latest_txn_time"),
        F.count("*").alias("row_count"),
    )
)

result = (
    df_accounts.alias("a")
    .join(transactions_by_account.alias("tba"), F.col("a.id") == F.col("tba.account_id"), "left")
    .select(F.col("a.balance"), F.col("tba.latest_txn_time"), F.col("tba.row_count"))
    .orderBy("latest_txn_time")
)
