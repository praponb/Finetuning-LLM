from pyspark.sql import functions as F

account_stats = (
    df_transactions
    .filter(F.col("account_id").isin([8, 23, 27, 28]))
    .groupBy("account_id")
    .agg(
        F.avg("amount").alias("avg_amount"),
        F.sum(F.when(F.col("merchant") == "Starbucks", F.col("amount")).otherwise(0)).alias("starbucks_amount"),
    )
)

result = (
    df_accounts.alias("a")
    .join(account_stats.alias("as"), F.col("a.id") == F.col("as.account_id"), "left")
    .select(
        F.col("a.id"),
        F.col("a.account_type"),
        F.col("as.avg_amount"),
        F.col("as.starbucks_amount"),
    )
)
