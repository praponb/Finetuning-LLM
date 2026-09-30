from pyspark.sql import functions as F

account_totals = (
    df_transactions
    .filter(F.col("is_flagged"))
    .groupBy("account_id")
    .agg(F.count("amount").alias("amount_count"))
)

result = (
    df_accounts.alias("a")
    .join(account_totals.alias("at"), F.col("a.id") == F.col("at.account_id"), "left")
    .select(F.col("a.id"), F.col("a.balance"), F.col("at.amount_count"))
    .orderBy(F.col("balance").desc())
)
