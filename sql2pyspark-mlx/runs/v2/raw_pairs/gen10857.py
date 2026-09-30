from pyspark.sql import functions as F

account_totals = (
    df_transactions
    .filter(F.col("amount").isNotNull())
    .groupBy("account_id")
    .agg(
        F.sum(F.when(F.col("merchant") == "Starbucks", 1).otherwise(0)).alias("starbucks_count"),
    )
)

result = (
    df_accounts.alias("a")
    .join(account_totals.alias("t"), F.col("a.id") == F.col("t.account_id"), "inner")
    .filter(F.col("t.starbucks_count") >= 52)
    .select(F.col("a.id"), F.col("a.account_type"), F.col("t.starbucks_count"))
    .orderBy("starbucks_count", "id")
    .limit(5)
)
