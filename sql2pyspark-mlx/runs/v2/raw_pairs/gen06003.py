from pyspark.sql import functions as F

account_stats = (
    df_transactions
    .groupBy("account_id")
    .agg(F.sum("amount").alias("total_amount"))
)

result = (
    df_accounts.alias("a")
    .join(account_stats.alias("A"), F.col("a.id") == F.col("A.account_id"), "left")
    .select(F.col("a.account_type"), F.col("A.total_amount"))
)
