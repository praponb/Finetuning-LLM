from pyspark.sql import functions as F

account_stats = (
    df_transactions
    .filter(F.col("is_flagged"))
    .groupBy("account_id")
    .agg(
        F.avg("amount").alias("avg_amount"),
        F.sum("amount").alias("total_amount"),
    )
)

result = (
    df_accounts.alias("a")
    .join(account_stats.alias("as"), F.col("a.id") == F.col("as.account_id"), "inner")
    .filter(F.col("as.total_amount") > 6000)
    .select(
        F.col("a.id"),
        F.col("a.account_type"),
        F.col("as.avg_amount"),
        F.col("as.total_amount"),
    )
    .orderBy(F.col("id").desc(), F.col("avg_amount").desc())
)
