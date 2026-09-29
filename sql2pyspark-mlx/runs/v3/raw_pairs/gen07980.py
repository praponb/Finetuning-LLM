from pyspark.sql import functions as F

transactions_by_account = (
    df_transactions
    .groupBy("account_id")
    .agg(
        F.count("amount").alias("amount_count"),
        F.avg("amount").alias("avg_amount"),
    )
)

result = (
    df_accounts.alias("a")
    .join(transactions_by_account.alias("tba"), F.col("a.id") == F.col("tba.account_id"), "inner")
    .filter(F.col("tba.avg_amount") >= 1800)
    .select(
        F.col("a.id"),
        F.col("a.status"),
        F.col("a.balance"),
        F.col("tba.amount_count"),
        F.col("tba.avg_amount"),
    )
    .orderBy("id")
    .limit(3)
)
