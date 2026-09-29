from pyspark.sql import functions as F

result = (
    df_transactions.alias("transactions")
    .join(df_accounts.alias("accounts"), F.col("transactions.account_id") == F.col("accounts.id"), "inner")
    .groupBy(F.col("accounts.account_type"))
    .agg(
        F.max(F.col("transactions.txn_time")).alias("latest_txn_time"),
        F.round(F.stddev(F.col("transactions.amount")), 2).alias("stddev_amount"),
    )
    .filter(F.col("stddev_amount") > 58)
    .orderBy(F.col("stddev_amount").desc())
)
