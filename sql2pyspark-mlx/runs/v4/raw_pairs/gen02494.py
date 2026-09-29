from pyspark.sql import functions as F

transactions_by_account = (
    df_transactions
    .filter(F.col("account_id").isin([12, 13, 17]))
    .groupBy("account_id")
    .agg(
        F.avg("amount").alias("avg_amount"),
        F.sum(F.when(F.col("merchant") == "Amazon", F.col("amount")).otherwise(0)).alias("amazon_amount"),
    )
)

result = (
    df_accounts.alias("a")
    .join(transactions_by_account.alias("tba"), F.col("a.id") == F.col("tba.account_id"), "inner")
    .select(
        F.col("a.id"),
        F.col("a.balance"),
        F.col("tba.avg_amount"),
        F.col("tba.amazon_amount"),
    )
)
