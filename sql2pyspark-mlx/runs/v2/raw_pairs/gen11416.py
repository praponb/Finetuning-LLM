from pyspark.sql import functions as F

transactions_by_account = (
    df_transactions
    .filter(F.col("is_flagged"))
    .groupBy("account_id")
    .agg(
        F.min("txn_time").alias("first_txn_time"),
        F.round(F.avg("amount"), 2).alias("avg_amount"),
    )
)

result = (
    df_accounts.alias("a")
    .join(transactions_by_account.alias("tba"), F.col("a.id") == F.col("tba.account_id"), "inner")
    .select(
        F.col("a.id"),
        F.col("a.status"),
        F.col("tba.first_txn_time"),
        F.col("tba.avg_amount"),
    )
    .orderBy("first_txn_time", "status", "id")
    .limit(10)
)
