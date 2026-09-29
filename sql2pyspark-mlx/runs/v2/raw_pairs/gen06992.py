from pyspark.sql import functions as F

account_summary = (
    df_transactions
    .filter(F.col("account_id").isin([22, 28, 31, 32]))
    .groupBy("account_id")
    .agg(F.countDistinct("amount").alias("unique_txn_types"))
)

result = (
    df_accounts.alias("a")
    .join(account_summary.alias("as"), F.col("a.id") == F.col("as.account_id"), "left")
    .filter(F.col("as.unique_txn_types") >= 5)
    .select(
        F.col("a.id"),
        F.col("a.balance"),
        F.col("a.status"),
        F.col("as.unique_txn_types"),
    )
    .orderBy(F.col("unique_txn_types").desc())
)
