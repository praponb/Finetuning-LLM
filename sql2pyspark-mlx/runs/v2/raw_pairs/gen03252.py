from pyspark.sql import functions as F

account_stats = (
    df_transactions
    .filter(F.col("merchant").isin(["7-Eleven", "Amazon"]))
    .groupBy("account_id")
    .agg(
        F.max("amount").alias("max_amount"),
        F.round(F.stddev("amount"), 2).alias("stddev_amount"),
    )
)

result = (
    df_accounts.alias("a")
    .join(account_stats.alias("t"), F.col("a.id") == F.col("t.account_id"), "inner")
    .select(
        F.col("a.id"),
        F.col("a.opened_date"),
        F.col("t.max_amount"),
        F.col("t.stddev_amount"),
    )
)
