from pyspark.sql import functions as F

account_stats = (
    df_transactions
    .groupBy("account_id")
    .agg(F.sum("amount").alias("total_amount"))
)

result = (
    df_accounts.alias("a")
    .join(account_stats.alias("as"), F.col("a.id") == F.col("as.account_id"), "left")
    .filter(F.col("as.total_amount") > 7000)
    .select(F.col("a.account_type"), F.col("a.balance"), F.col("as.total_amount"))
    .orderBy(F.col("balance").desc())
)
