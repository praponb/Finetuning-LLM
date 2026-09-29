from pyspark.sql import functions as F

account_stats = (
    df_transactions
    .groupBy("account_id")
    .agg(
        F.sum(F.when(F.col("merchant") == "Amazon", F.col("amount")).otherwise(0)).alias("amazon_amount"),
        (F.max("amount") - F.min("amount")).alias("amount_range"),
    )
)

result = (
    df_accounts.alias("a")
    .join(account_stats.alias("as"), F.col("a.id") == F.col("as.account_id"), "inner")
    .select(
        F.col("a.id"),
        F.col("a.opened_date"),
        F.col("as.amazon_amount"),
        F.col("as.amount_range"),
    )
    .orderBy("id")
    .limit(50)
)
