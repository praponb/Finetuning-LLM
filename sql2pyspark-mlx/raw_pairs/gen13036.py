from pyspark.sql import functions as F

account_totals = (
    df_transactions
    .filter(F.col("amount") > 3700)
    .groupBy("account_id")
    .agg(
        F.sort_array(F.collect_set("merchant")).alias("merchant_list"),
        F.max("txn_time").alias("last_txn_time"),
    )
)

result = (
    df_accounts.alias("a")
    .join(account_totals.alias("at"), F.col("a.id") == F.col("at.account_id"), "inner")
    .select(
        F.col("a.id"),
        F.col("a.status"),
        F.col("at.merchant_list"),
        F.col("at.last_txn_time"),
    )
    .orderBy("last_txn_time")
)
