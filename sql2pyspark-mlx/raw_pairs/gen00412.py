from pyspark.sql import functions as F

account_stats = (
    df_transactions
    .groupBy("account_id")
    .agg(F.round(F.avg("amount"), 2).alias("avg_amount"))
)

result = (
    df_accounts.alias("a")
    .join(account_stats.alias("as"), F.col("a.id") == F.col("as.account_id"), "inner")
    .filter(F.col("as.avg_amount") > 75)
    .select(F.col("a.customer_id"), F.col("a.balance"), F.col("as.avg_amount"))
    .orderBy(F.col("avg_amount").desc())
)
