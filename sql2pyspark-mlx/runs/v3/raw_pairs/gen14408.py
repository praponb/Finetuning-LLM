from pyspark.sql import functions as F

account_summary = (
    df_transactions
    .filter(F.col("amount") >= 1700)
    .groupBy("account_id")
    .agg(
        F.sum(F.when(F.col("merchant") == "Starbucks", 1).otherwise(0)).alias("starbucks_count"),
    )
)

result = (
    df_accounts.alias("a")
    .join(account_summary.alias("t"), F.col("a.id") == F.col("t.account_id"), "inner")
    .select(F.col("a.id"), F.col("a.account_type"), F.col("t.starbucks_count"))
    .orderBy(F.col("starbucks_count").desc(), F.col("id").desc())
)
