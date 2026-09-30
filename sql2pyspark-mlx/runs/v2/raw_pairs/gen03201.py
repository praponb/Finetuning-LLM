from pyspark.sql import functions as F

account_stats = (
    df_transactions
    .filter(F.to_date("txn_time") >= "2024-05-01")
    .groupBy("account_id")
    .agg(
        F.count("*").alias("total_count"),
        F.min("amount").alias("min_amount"),
    )
)

result = (
    df_accounts.alias("a")
    .join(account_stats.alias("as"), F.col("a.id") == F.col("as.account_id"), "left")
    .select(F.col("a.balance"), F.col("as.total_count"), F.col("as.min_amount"))
    .orderBy(F.col("total_count").desc())
)
