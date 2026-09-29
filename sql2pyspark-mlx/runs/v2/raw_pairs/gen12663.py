from pyspark.sql import functions as F

transactions_by_account = (
    df_transactions
    .filter(F.col("amount") > 1500)
    .groupBy("account_id")
    .agg(
        F.avg("amount").alias("avg_amount"),
        F.round(F.stddev("amount"), 2).alias("stddev_amount"),
    )
)

result = (
    df_accounts.alias("a")
    .join(transactions_by_account.alias("t"), F.col("a.id") == F.col("t.account_id"), "left")
    .select(
        F.col("a.id"),
        F.col("a.customer_id"),
        F.col("t.avg_amount"),
        F.col("t.stddev_amount"),
    )
)
