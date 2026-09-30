from pyspark.sql import functions as F

account_stats = (
    df_transactions
    .filter(F.col("account_id").isin([4, 30, 36]))
    .groupBy("account_id")
    .agg(
        F.round(F.stddev("amount"), 2).alias("stddev_amount"),
        F.min("amount").alias("min_amount"),
    )
)

result = (
    df_accounts.alias("a")
    .join(account_stats.alias("as"), F.col("a.id") == F.col("as.account_id"), "inner")
    .select(
        F.col("a.id"),
        F.col("a.status"),
        F.col("as.stddev_amount"),
        F.col("as.min_amount"),
    )
    .orderBy("status", "id")
    .limit(3)
)
