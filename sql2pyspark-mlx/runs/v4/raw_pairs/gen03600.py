from pyspark.sql import functions as F

account_summary = (
    df_transactions
    .filter(F.col("amount") < 2000)
    .groupBy("account_id")
    .agg(
        F.countDistinct("amount").alias("unique_txn_types"),
        F.max("txn_time").alias("last_txn_time"),
    )
)

result = (
    df_accounts.alias("a")
    .join(account_summary.alias("as"), F.col("a.id") == F.col("as.account_id"), "left")
    .filter(F.col("as.unique_txn_types") >= 5)
    .select(
        F.col("a.id"),
        F.col("a.account_type"),
        F.col("as.unique_txn_types"),
        F.col("as.last_txn_time"),
    )
)
