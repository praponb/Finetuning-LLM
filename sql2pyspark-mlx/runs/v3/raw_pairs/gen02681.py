from pyspark.sql import functions as F

transactions_by_account = (
    df_transactions
    .groupBy("account_id")
    .agg(
        F.min("amount").alias("min_amount"),
        F.max("txn_time").alias("latest_txn_time"),
    )
)

result = (
    df_accounts.alias("a")
    .join(transactions_by_account.alias("t"), F.col("a.id") == F.col("t.account_id"), "inner")
    .filter(F.col("t.min_amount") > 2900)
    .select(F.col("a.account_type"), F.col("t.min_amount"), F.col("t.latest_txn_time"))
    .orderBy(F.col("latest_txn_time").desc())
)
