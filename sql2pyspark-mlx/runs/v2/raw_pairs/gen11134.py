from pyspark.sql import functions as F

account_stats = (
    df_transactions
    .filter(F.to_date("txn_time") < "2023-08-01")
    .groupBy("account_id")
    .agg(
        F.avg("amount").alias("avg_amount"),
        F.count("amount").alias("amount_count"),
    )
)

result = (
    df_accounts.alias("a")
    .join(account_stats.alias("as"), F.col("a.id") == F.col("as.account_id"), "inner")
    .filter(F.col("as.amount_count") >= 6)
    .select(
        F.col("a.id"),
        F.col("a.status"),
        F.col("a.balance"),
        F.col("as.avg_amount"),
        F.col("as.amount_count"),
    )
    .orderBy(F.col("status").desc())
)
