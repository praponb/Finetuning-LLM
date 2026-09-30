from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_transactions.alias("t"), F.col("t.account_id") == F.col("a.id"), "left")
    .filter(F.col("t.is_flagged"))
    .groupBy(F.col("a.id"), F.col("a.status"))
    .agg(
        F.count(F.col("t.id")).alias("transaction_count"),
        F.sum(F.when(F.col("t.merchant") == "Starbucks", F.col("t.amount")).otherwise(0)).alias("starbucks_amount"),
        F.round(((100 * F.sum(F.when(F.col("t.merchant") == "7-Eleven", 1).otherwise(0))) / F.count("*")), 1).alias("pct_7_eleven"),
    )
)
