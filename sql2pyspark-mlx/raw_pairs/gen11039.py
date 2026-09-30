from pyspark.sql import functions as F

account_summary = (
    df_transactions
    .filter(F.to_date("txn_time") >= "2025-01-01")
    .groupBy("account_id")
    .agg(F.round(F.avg("amount"), 2).alias("avg_amount"))
)

result = (
    df_accounts.alias("a")
    .join(account_summary.alias("as"), F.col("a.id") == F.col("as.account_id"), "left")
    .filter(F.col("as.avg_amount") >= 75)
    .select(F.col("a.id"), F.col("a.account_type"), F.col("as.avg_amount"))
    .orderBy(F.col("id").desc())
)
