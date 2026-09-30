from pyspark.sql import functions as F

account_stats = (
    df_transactions
    .filter(F.col("merchant").isin(["Grab", "Shell"]))
    .groupBy("account_id")
    .agg(F.round(F.stddev("amount"), 2).alias("stddev_amount"))
)

result = (
    df_accounts.alias("a")
    .join(account_stats.alias("as"), F.col("a.id") == F.col("as.account_id"), "inner")
    .filter(F.col("as.stddev_amount") >= 25)
    .select(F.col("a.status"), F.col("a.customer_id"), F.col("as.stddev_amount"))
    .orderBy(F.col("customer_id").desc())
)
