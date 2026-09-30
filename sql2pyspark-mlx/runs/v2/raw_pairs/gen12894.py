from pyspark.sql import functions as F

transactions_by_account = (
    df_transactions
    .filter(F.to_date("txn_time") >= "2024-06-18")
    .groupBy("account_id")
    .agg(F.round(F.avg("amount"), 2).alias("avg_amount"))
)

result = (
    df_accounts.alias("a")
    .join(transactions_by_account.alias("tba"), F.col("a.id") == F.col("tba.account_id"), "left")
    .select(F.col("a.id"), F.col("a.status"), F.col("tba.avg_amount"))
)
