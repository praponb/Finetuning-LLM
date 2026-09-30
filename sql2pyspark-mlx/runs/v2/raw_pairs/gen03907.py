from pyspark.sql import functions as F

account_totals = (
    df_transactions
    .filter(F.col("account_id").isin([13, 27, 40]))
    .groupBy("account_id")
    .agg(F.count(F.when(F.col("txn_type") == "DEBIT", 1)).alias("debit_count"))
)

result = (
    df_accounts.alias("a")
    .join(account_totals.alias("t"), F.col("a.id") == F.col("t.account_id"), "inner")
    .select(F.col("a.balance"), F.col("t.debit_count"))
)
