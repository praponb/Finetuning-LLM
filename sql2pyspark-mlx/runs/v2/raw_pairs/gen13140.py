from pyspark.sql import functions as F

account_summary = (
    df_transactions
    .groupBy("account_id")
    .agg(F.sum("amount").alias("total_amount"))
)

result = (
    df_accounts.alias("a")
    .join(account_summary.alias("as"), F.col("a.id") == F.col("as.account_id"), "inner")
    .filter(F.col("as.total_amount") >= 13000)
    .select(F.col("a.account_type"), F.col("a.opened_date"), F.col("as.total_amount"))
)
