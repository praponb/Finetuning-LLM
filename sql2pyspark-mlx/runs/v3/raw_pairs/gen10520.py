from pyspark.sql import functions as F

account_stats = (
    df_transactions
    .filter(F.col("merchant") == "Amazon")
    .groupBy("account_id")
    .agg(F.max("amount").alias("max_amount"))
)

result = (
    df_accounts.alias("a")
    .join(account_stats.alias("as"), F.col("a.id") == F.col("as.account_id"), "left")
    .select(F.col("a.id"), F.col("a.customer_id"), F.col("as.max_amount"))
    .orderBy(F.col("max_amount").desc())
)
