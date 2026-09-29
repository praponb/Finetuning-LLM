from pyspark.sql import functions as F

account_summary = (
    df_transactions
    .filter(F.col("account_id").isin([15, 26]))
    .groupBy("account_id")
    .agg(F.round(F.avg("amount"), 2).alias("avg_amount"))
)

result = (
    df_accounts.alias("a")
    .join(account_summary.alias("A"), F.col("a.id") == F.col("A.account_id"), "inner")
    .select(F.col("a.customer_id"), F.col("A.avg_amount"))
)
