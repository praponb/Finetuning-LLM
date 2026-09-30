from pyspark.sql import functions as F

account_stats = (
    df_transactions
    .filter(F.col("txn_type") == "DEBIT")
    .groupBy("account_id")
    .agg(F.count("*").alias("total_count"))
)

result = (
    df_accounts.alias("a")
    .join(account_stats.alias("as"), F.col("a.id") == F.col("as.account_id"), "inner")
    .filter(F.col("as.total_count") >= 5)
    .select(F.col("a.id"), F.col("a.customer_id"), F.col("as.total_count"))
    .orderBy(F.col("customer_id").desc(), F.col("total_count").desc())
)
