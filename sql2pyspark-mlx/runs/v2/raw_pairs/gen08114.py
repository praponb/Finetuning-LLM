from pyspark.sql import functions as F

account_stats = (
    df_transactions
    .filter(F.col("amount").between(2600, 3800))
    .groupBy("account_id")
    .agg(F.round(F.avg("amount"), 2).alias("avg_amount"))
)

result = (
    df_accounts.alias("a")
    .join(account_stats.alias("A"), F.col("a.id") == F.col("A.account_id"), "inner")
    .filter(F.col("A.avg_amount") >= 30)
    .select(F.col("a.status"), F.col("A.avg_amount"))
)
