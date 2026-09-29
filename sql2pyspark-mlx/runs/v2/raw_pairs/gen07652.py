from pyspark.sql import functions as F

account_stats = (
    df_transactions
    .groupBy("account_id")
    .agg(F.count(F.when(F.col("txn_type") == "CREDIT", 1)).alias("credit_count"))
)

result = (
    df_accounts.alias("a")
    .join(account_stats.alias("as"), F.col("a.id") == F.col("as.account_id"), "inner")
    .select(F.col("a.account_type"), F.col("a.opened_date"), F.col("as.credit_count"))
    .orderBy("credit_count")
)
