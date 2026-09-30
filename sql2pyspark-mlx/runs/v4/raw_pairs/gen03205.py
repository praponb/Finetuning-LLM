from pyspark.sql import functions as F

account_stats = (
    df_transactions
    .filter(F.col("amount").between(1100, 2500))
    .groupBy("account_id")
    .agg(F.countDistinct("amount").alias("unique_accounts"))
)

result = (
    df_accounts.alias("a")
    .join(account_stats.alias("as"), F.col("a.id") == F.col("as.account_id"), "inner")
    .filter(F.col("as.unique_accounts") >= 6)
    .select(F.col("a.id"), F.col("a.balance"), F.col("as.unique_accounts"))
)
