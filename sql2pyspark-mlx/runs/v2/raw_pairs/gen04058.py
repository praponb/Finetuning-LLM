from pyspark.sql import functions as F

account_totals = (
    df_transactions
    .groupBy("account_id")
    .agg(F.count(F.when(F.col("txn_type") == "FEE", 1)).alias("fee_count"))
)

result = (
    df_accounts.alias("a")
    .join(account_totals.alias("t"), F.col("a.id") == F.col("t.account_id"), "inner")
    .select(F.col("a.account_type"), F.col("t.fee_count"))
    .orderBy("account_type", "fee_count")
)
