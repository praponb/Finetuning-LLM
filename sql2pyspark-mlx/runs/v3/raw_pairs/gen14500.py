from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_transactions.alias("t"), F.col("a.id") == F.col("t.account_id"), "left")
    .filter(F.col("a.account_type") == "LOAN")
    .groupBy(F.col("a.id"), F.col("a.account_type"))
    .agg(
        F.count(F.col("t.id")).alias("transaction_count"),
        F.round(F.stddev(F.col("t.amount")), 2).alias("stddev_amount"),
        F.countDistinct(F.col("t.amount")).alias("unique_txn_types"),
    )
    .orderBy(F.col("unique_txn_types").desc())
)
