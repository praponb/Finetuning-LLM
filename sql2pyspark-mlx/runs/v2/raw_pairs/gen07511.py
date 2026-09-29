from pyspark.sql import functions as F

transactions_by_account = (
    df_transactions
    .filter(F.col("txn_time") >= "2025-05-06")
    .groupBy("account_id")
    .agg(
        F.count(F.when(F.col("txn_type") == "DEBIT", 1)).alias("debit_count"),
        F.min("amount").alias("min_amount"),
    )
)

result = (
    df_accounts.alias("a")
    .join(transactions_by_account.alias("tba"), F.col("a.id") == F.col("tba.account_id"), "inner")
    .select(F.col("a.account_type"), F.col("tba.debit_count"), F.col("tba.min_amount"))
    .orderBy(F.col("min_amount").desc())
)
