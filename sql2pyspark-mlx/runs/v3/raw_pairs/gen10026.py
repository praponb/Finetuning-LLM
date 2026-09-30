from pyspark.sql import functions as F

transactions_by_account = (
    df_transactions
    .groupBy("account_id")
    .agg(
        F.min("txn_time").alias("first_txn_time"),
        F.count("*").alias("cnt"),
    )
)

result = (
    df_accounts.alias("a")
    .join(transactions_by_account.alias("T"), F.col("a.id") == F.col("T.account_id"), "left")
    .filter(F.col("T.cnt") > 8)
    .select(
        F.col("a.balance"),
        F.col("a.customer_id"),
        F.col("T.first_txn_time"),
        F.col("T.cnt"),
    )
    .orderBy(F.col("balance").desc())
)
