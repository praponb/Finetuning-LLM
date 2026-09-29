from pyspark.sql import functions as F

account_stats = (
    df_transactions
    .groupBy("account_id")
    .agg(F.countDistinct("amount").alias("unique_txn_types"))
)

result = (
    df_accounts.alias("a")
    .join(account_stats.alias("t"), F.col("a.id") == F.col("t.account_id"), "inner")
    .filter(F.col("t.unique_txn_types") > 9)
    .select(F.col("a.customer_id"), F.col("a.status"), F.col("t.unique_txn_types"))
    .orderBy("status")
)
