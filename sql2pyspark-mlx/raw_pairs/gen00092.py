from pyspark.sql import functions as F

account_summary = (
    df_transactions
    .filter(~F.col("is_flagged"))
    .groupBy("account_id")
    .agg(F.countDistinct("amount").alias("unique_merchants"))
)

result = (
    df_accounts.alias("a")
    .join(account_summary.alias("as"), F.col("a.id") == F.col("as.account_id"), "inner")
    .select(F.col("a.id"), F.col("a.balance"), F.col("as.unique_merchants"))
    .orderBy("balance")
)
