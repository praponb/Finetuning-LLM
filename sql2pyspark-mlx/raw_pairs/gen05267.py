from pyspark.sql import functions as F

account_stats = (
    df_transactions
    .groupBy("account_id")
    .agg(
        F.max("amount").alias("max_amount"),
        F.round(((100 * F.sum(F.when(F.col("merchant") == "7-Eleven", 1).otherwise(0))) / F.count("*")), 1).alias("pct_7_eleven"),
    )
)

result = (
    df_accounts.alias("a")
    .join(account_stats.alias("t"), F.col("a.id") == F.col("t.account_id"), "inner")
    .filter(F.col("t.pct_7_eleven") >= 78)
    .select(
        F.col("a.customer_id"),
        F.col("a.status"),
        F.col("t.max_amount"),
        F.col("t.pct_7_eleven"),
    )
    .orderBy(F.col("customer_id").desc())
)
