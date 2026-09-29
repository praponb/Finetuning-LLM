from pyspark.sql import functions as F

account_stats = (
    df_transactions
    .groupBy("account_id")
    .agg(F.count("*").alias("row_count"))
)

result = (
    df_accounts.alias("a")
    .join(account_stats.alias("as"), F.col("a.id") == F.col("as.account_id"), "inner")
    .select(
        F.col("a.id"),
        F.col("a.account_type"),
        F.col("a.status"),
        F.col("as.row_count"),
    )
    .orderBy(F.col("account_type").desc(), "id")
    .limit(3)
)
