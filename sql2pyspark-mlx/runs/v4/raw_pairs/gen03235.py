from pyspark.sql import functions as F

account_summary = (
    df_transactions
    .filter(F.col("merchant").isin(["Shell", "Starbucks"]))
    .groupBy("account_id")
    .agg(F.round(F.stddev("amount"), 2).alias("stddev_amount"))
)

result = (
    df_accounts.alias("a")
    .join(account_summary.alias("as"), F.col("a.id") == F.col("as.account_id"), "inner")
    .filter(F.col("as.stddev_amount") >= 38)
    .select(F.col("a.balance"), F.col("as.stddev_amount"))
    .orderBy(F.col("balance").desc())
)
