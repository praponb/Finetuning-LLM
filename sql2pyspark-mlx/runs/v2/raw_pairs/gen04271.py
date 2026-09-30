from pyspark.sql import functions as F

account_totals = (
    df_transactions
    .filter(F.col("txn_type") == "DEBIT")
    .groupBy("account_id")
    .agg(F.count("amount").alias("amount_count"))
)

result = (
    df_accounts.alias("a")
    .join(account_totals.alias("at"), F.col("a.id") == F.col("at.account_id"), "inner")
    .select(F.col("a.id"), F.col("a.account_type"), F.col("at.amount_count"))
)
