from pyspark.sql import functions as F

transactions_by_account = (
    df_transactions
    .filter(F.col("account_id").isin([34, 35, 36]))
    .groupBy("account_id")
    .agg(F.avg("amount").alias("avg_amount"))
)

result = (
    df_accounts.alias("a")
    .join(transactions_by_account.alias("t"), F.col("a.id") == F.col("t.account_id"), "inner")
    .select(F.col("a.id"), F.col("a.account_type"), F.col("t.avg_amount"))
    .orderBy("account_type", "id")
    .limit(10)
)
