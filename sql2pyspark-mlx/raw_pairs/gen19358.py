from pyspark.sql import functions as F

account_stats = (
    df_transactions
    .filter(F.col("merchant").isin(["7-Eleven", "Grab", "Starbucks"]))
    .groupBy("account_id")
    .agg(F.countDistinct("amount").alias("unique_txn_types"))
)

result = (
    df_accounts.alias("a")
    .join(account_stats.alias("as"), F.col("a.id") == F.col("as.account_id"), "inner")
    .filter(F.col("as.unique_txn_types") >= 7)
    .select(
        F.col("a.account_type"),
        F.col("a.opened_date"),
        F.col("as.unique_txn_types"),
    )
    .orderBy(F.col("account_type").desc())
)
