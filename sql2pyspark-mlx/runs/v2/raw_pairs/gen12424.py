from pyspark.sql import functions as F

transactions_by_account = (
    df_transactions
    .groupBy("account_id")
    .agg(
        F.countDistinct("amount").alias("unique_merchants"),
        F.min("txn_time").alias("first_txn_time"),
    )
)

result = (
    df_accounts.alias("a")
    .join(transactions_by_account.alias("tba"), F.col("a.id") == F.col("tba.account_id"), "inner")
    .select(
        F.col("a.id"),
        F.col("a.opened_date"),
        F.col("tba.unique_merchants"),
        F.col("tba.first_txn_time"),
    )
)
