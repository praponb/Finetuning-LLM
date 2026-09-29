from pyspark.sql import functions as F

transactions_by_account = (
    df_transactions
    .groupBy("account_id")
    .agg(F.round(F.stddev("amount"), 2).alias("stddev_amount"))
)

result = (
    df_accounts.alias("a")
    .join(transactions_by_account.alias("t"), F.col("a.id") == F.col("t.account_id"), "left")
    .select(
        F.col("a.id"),
        F.col("a.opened_date"),
        F.col("a.account_type"),
        F.col("t.stddev_amount"),
    )
    .orderBy("opened_date")
)
