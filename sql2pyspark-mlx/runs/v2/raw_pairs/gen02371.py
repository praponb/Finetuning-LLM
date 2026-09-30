from pyspark.sql import functions as F

transactions_by_account = (
    df_transactions
    .filter(F.col("amount") < 2800)
    .groupBy("account_id")
    .agg(
        F.avg("amount").alias("avg_amount"),
        F.count("*").alias("transaction_count"),
    )
)

result = (
    df_accounts.alias("a")
    .join(transactions_by_account.alias("tba"), F.col("a.id") == F.col("tba.account_id"), "inner")
    .filter(F.col("tba.transaction_count") > 4)
    .select(F.col("a.balance"), F.col("tba.avg_amount"), F.col("tba.transaction_count"))
)
