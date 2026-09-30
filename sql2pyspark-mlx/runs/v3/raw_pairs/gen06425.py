from pyspark.sql import functions as F

account_totals = (
    df_transactions
    .groupBy("account_id")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("merchant") == "7-Eleven", 1).otherwise(0))) / F.count("*")), 1).alias("pct_7_eleven"),
        F.avg("amount").alias("avg_amount"),
    )
)

result = (
    df_accounts.alias("accounts")
    .join(account_totals.alias("t"), F.col("accounts.id") == F.col("t.account_id"), "left")
    .filter(F.col("t.pct_7_eleven") >= 37)
    .select(
        F.col("accounts.account_type"),
        F.col("t.pct_7_eleven"),
        F.col("t.avg_amount"),
    )
)
