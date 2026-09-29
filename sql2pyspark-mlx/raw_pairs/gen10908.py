from pyspark.sql import functions as F

transactions_by_account = (
    df_transactions
    .groupBy("account_id")
    .agg(
        F.round(F.avg("amount"), 2).alias("avg_amount"),
        F.sum("amount").alias("total_amount"),
    )
)

result = (
    df_accounts.alias("a")
    .join(transactions_by_account.alias("T"), F.col("a.id") == F.col("T.account_id"), "inner")
    .select(
        F.col("a.opened_date"),
        F.col("a.customer_id"),
        F.col("T.avg_amount"),
        F.col("T.total_amount"),
    )
)
