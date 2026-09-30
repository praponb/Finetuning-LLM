from pyspark.sql import functions as F

account_stats = (
    df_transactions
    .filter(F.col("account_id") == 33)
    .groupBy("account_id")
    .agg(F.min("amount").alias("min_amount"))
)

result = (
    df_accounts.alias("a")
    .join(account_stats.alias("as"), F.col("a.id") == F.col("as.account_id"), "inner")
    .filter(F.col("as.min_amount") > 1300)
    .select(F.col("a.id"), F.col("a.opened_date"), F.col("as.min_amount"))
    .orderBy("opened_date", F.col("min_amount").desc())
)
