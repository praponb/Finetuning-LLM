from pyspark.sql import functions as F

transactions_by_account = (
    df_transactions
    .filter(F.col("merchant").isin(["Lazada", "Shell"]))
    .groupBy("account_id")
    .agg(F.max("txn_time").alias("latest_txn_time"))
)

result = (
    df_accounts.alias("a")
    .join(transactions_by_account.alias("tba"), F.col("a.id") == F.col("tba.account_id"), "left")
    .select(
        F.col("a.id"),
        F.col("a.account_type"),
        F.col("a.opened_date"),
        F.col("tba.latest_txn_time"),
    )
    .orderBy("latest_txn_time")
)
