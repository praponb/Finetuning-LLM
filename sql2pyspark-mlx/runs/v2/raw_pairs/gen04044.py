from pyspark.sql import functions as F

account_stats = (
    df_transactions
    .filter(~F.col("is_flagged"))
    .groupBy("account_id")
    .agg(F.count("*").alias("cnt"))
)

result = (
    df_accounts.alias("accounts")
    .join(account_stats.alias("account_stats"), F.col("accounts.id") == F.col("account_stats.account_id"), "left")
    .filter(F.col("account_stats.cnt") > 9)
    .select(F.col("accounts.id"), F.col("accounts.balance"), F.col("account_stats.cnt"))
    .orderBy("id")
)
