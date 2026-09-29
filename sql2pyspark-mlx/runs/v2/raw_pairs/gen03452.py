from pyspark.sql import functions as F

account_totals = (
    df_transactions
    .groupBy("account_id")
    .agg(
        F.countDistinct("amount").alias("unique_txn_types"),
        F.min("txn_time").alias("min_txn_time"),
    )
)

result = (
    df_accounts.alias("a")
    .join(account_totals.alias("t"), F.col("a.id") == F.col("t.account_id"), "left")
    .filter(F.col("t.unique_txn_types") >= 6)
    .select(
        F.col("a.id"),
        F.col("a.balance"),
        F.col("a.status"),
        F.col("t.unique_txn_types"),
        F.col("t.min_txn_time"),
    )
    .orderBy(F.col("status").desc(), F.col("unique_txn_types").desc())
)
