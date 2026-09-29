from pyspark.sql import functions as F

account_stats = (
    df_transactions
    .groupBy("account_id")
    .agg(
        F.max("txn_time").alias("last_txn_time"),
        F.sort_array(F.collect_set("merchant")).alias("merchant_list"),
    )
)

result = (
    df_accounts.alias("a")
    .join(account_stats.alias("A"), F.col("a.id") == F.col("A.account_id"), "inner")
    .select(
        F.col("a.id"),
        F.col("a.status"),
        F.col("A.last_txn_time"),
        F.col("A.merchant_list"),
    )
)
