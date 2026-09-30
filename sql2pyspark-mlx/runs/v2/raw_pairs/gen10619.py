from pyspark.sql import functions as F

account_totals = (
    df_transactions
    .filter(F.hour("txn_time").between(8, 11))
    .groupBy("account_id")
    .agg(F.count(F.when(F.col("txn_type") == "FEE", 1)).alias("fee_count"))
)

result = (
    df_accounts.alias("a")
    .join(account_totals.alias("t"), F.col("a.id") == F.col("t.account_id"), "left")
    .filter(F.col("t.fee_count") >= 6)
    .select(F.col("a.id"), F.col("a.status"), F.col("a.balance"), F.col("t.fee_count"))
    .orderBy(F.col("balance").desc())
)
