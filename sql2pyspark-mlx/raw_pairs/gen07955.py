from pyspark.sql import functions as F

account_totals = (
    df_transactions
    .groupBy("account_id")
    .agg(F.count(F.when(F.col("merchant") == "Amazon", 1)).alias("amazon_count"))
)

result = (
    df_accounts.alias("a")
    .join(account_totals.alias("t"), F.col("a.id") == F.col("t.account_id"), "inner")
    .filter(F.col("t.amazon_count") > 6)
    .select(F.col("a.status"), F.col("t.amazon_count"))
    .orderBy(F.col("status").desc())
)
