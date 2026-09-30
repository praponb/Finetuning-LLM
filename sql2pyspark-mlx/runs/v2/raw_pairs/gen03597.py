from pyspark.sql import functions as F

account_stats = (
    df_transactions
    .filter(~F.col("merchant").isin(["Shell", "Grab"]))
    .groupBy("account_id")
    .agg(
        F.round(F.stddev("amount"), 2).alias("stddev_amount"),
        F.round(((100 * F.sum(F.when(F.col("merchant") == "7-Eleven", 1).otherwise(0))) / F.count("*")), 1).alias("pct_7_eleven"),
    )
)

result = (
    df_accounts.alias("a")
    .join(account_stats.alias("as"), F.col("a.id") == F.col("as.account_id"), "inner")
    .select(
        F.col("a.id"),
        F.col("a.customer_id"),
        F.col("as.stddev_amount"),
        F.col("as.pct_7_eleven"),
    )
    .orderBy(F.col("stddev_amount").desc())
)
