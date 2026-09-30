from pyspark.sql import functions as F

transactions_by_account = (
    df_transactions
    .groupBy("account_id")
    .agg(F.max("txn_time").alias("last_txn_time"))
)

result = (
    df_accounts.alias("a")
    .join(transactions_by_account.alias("T"), F.col("a.id") == F.col("T.account_id"), "inner")
    .select(F.col("a.id"), F.col("a.balance"), F.col("T.last_txn_time"))
    .orderBy(F.col("id").desc())
)
