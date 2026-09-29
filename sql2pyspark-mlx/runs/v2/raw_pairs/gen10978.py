from pyspark.sql import functions as F

account_summary = (
    df_transactions
    .groupBy("account_id")
    .agg(
        F.sum(F.when(F.col("merchant") == "Amazon", F.col("amount")).otherwise(0)).alias("amazon_amount"),
        F.count(F.when(F.col("merchant") == "7-Eleven", 1)).alias("7_eleven_count"),
    )
)

result = (
    df_accounts.alias("a")
    .join(account_summary.alias("as"), F.col("a.id") == F.col("as.account_id"), "inner")
    .select(
        F.col("a.opened_date"),
        F.col("a.balance"),
        F.col("as.amazon_amount"),
        F.col("as.7_eleven_count"),
    )
)
