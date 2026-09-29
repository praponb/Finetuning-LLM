from pyspark.sql import functions as F

account_stats = (
    df_transactions
    .filter(F.col("txn_type") == "DEBIT")
    .groupBy("account_id")
    .agg(
        F.avg("amount").alias("avg_amount"),
        F.countDistinct("amount").alias("unique_txn_types"),
    )
)

result = (
    df_accounts.alias("a")
    .join(account_stats.alias("as"), F.col("a.id") == F.col("as.account_id"), "inner")
    .select(
        F.col("a.opened_date"),
        F.col("as.avg_amount"),
        F.col("as.unique_txn_types"),
    )
)
