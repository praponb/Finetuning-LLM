from pyspark.sql import functions as F

account_totals = (
    df_transactions
    .filter(F.to_date("txn_time") >= "2025-06-20")
    .groupBy("account_id")
    .agg(F.count("amount").alias("amount_count"))
)

result = (
    df_accounts.alias("a")
    .join(account_totals.alias("at"), F.col("a.id") == F.col("at.account_id"), "inner")
    .filter(F.col("at.amount_count") > 5)
    .select(F.col("a.opened_date"), F.col("at.amount_count"))
    .orderBy(F.col("opened_date").desc())
)
