from pyspark.sql import functions as F

transactions_by_account = (
    df_transactions
    .filter(F.col("account_id").isin([5, 11, 17, 22]))
    .groupBy("account_id")
    .agg(F.max("txn_time").alias("last_txn_time"))
)

result = (
    df_accounts.alias("a")
    .join(transactions_by_account.alias("t"), F.col("a.id") == F.col("t.account_id"), "left")
    .select(
        F.col("a.id"),
        F.col("a.account_type"),
        F.col("a.customer_id"),
        F.col("t.last_txn_time"),
    )
    .orderBy("account_type")
)
