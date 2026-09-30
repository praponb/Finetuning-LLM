from pyspark.sql import functions as F

account_summary = (
    df_transactions
    .filter(F.col("is_flagged"))
    .groupBy("account_id")
    .agg(
        F.count(F.when(F.col("merchant") == "Lazada", 1)).alias("lazada_count"),
        F.round(F.avg("amount"), 2).alias("avg_amount"),
    )
)

result = (
    df_accounts.alias("a")
    .join(account_summary.alias("as"), F.col("a.id") == F.col("as.account_id"), "inner")
    .filter(F.col("as.avg_amount") > 23)
    .select(
        F.col("a.id"),
        F.col("a.balance"),
        F.col("as.lazada_count"),
        F.col("as.avg_amount"),
    )
    .orderBy("id")
)
