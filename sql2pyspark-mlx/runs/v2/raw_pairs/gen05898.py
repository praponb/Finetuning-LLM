from pyspark.sql import functions as F

transactions_by_account = (
    df_transactions
    .filter(F.to_date("txn_time") < "2023-05-01")
    .groupBy("account_id")
    .agg(
        F.round(F.stddev("amount"), 2).alias("stddev_amount"),
        F.avg("amount").alias("avg_amount"),
    )
)

result = (
    df_accounts.alias("a")
    .join(transactions_by_account.alias("tba"), F.col("a.id") == F.col("tba.account_id"), "inner")
    .select(
        F.col("a.id"),
        F.col("a.opened_date"),
        F.col("a.customer_id"),
        F.col("tba.stddev_amount"),
        F.col("tba.avg_amount"),
    )
    .orderBy("avg_amount")
)
