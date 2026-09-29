from pyspark.sql import functions as F

transactions_by_account = (
    df_transactions
    .filter(F.col("account_id").isin([2, 21]))
    .groupBy("account_id")
    .agg(
        F.min("txn_time").alias("first_txn_time"),
        F.count(F.when(F.col("txn_type") == "CREDIT", 1)).alias("credit_count"),
    )
)

result = (
    df_accounts.alias("a")
    .join(transactions_by_account.alias("tba"), F.col("a.id") == F.col("tba.account_id"), "inner")
    .select(
        F.col("a.account_type"),
        F.col("tba.first_txn_time"),
        F.col("tba.credit_count"),
    )
    .orderBy("account_type", F.col("first_txn_time").desc())
)
