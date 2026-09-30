from pyspark.sql import functions as F

account_stats = (
    df_transactions
    .filter(F.col("amount").between(3000, 3700))
    .groupBy("account_id")
    .agg(F.countDistinct("amount").alias("unique_txn_types"))
)

result = (
    df_accounts.alias("a")
    .join(account_stats.alias("as"), F.col("a.id") == F.col("as.account_id"), "inner")
    .filter(F.col("as.unique_txn_types") >= 5)
    .select(
        F.col("a.opened_date"),
        F.col("a.customer_id"),
        F.col("as.unique_txn_types"),
    )
)
