from pyspark.sql import functions as F

account_totals = (
    df_transactions
    .groupBy("account_id")
    .agg(
        F.sum(F.when(F.col("merchant") == "Lazada", 1).otherwise(0)).alias("lazada_count"),
        F.min("txn_time").alias("first_txn_time"),
    )
)

result = (
    df_accounts.alias("a")
    .join(account_totals.alias("at"), F.col("a.id") == F.col("at.account_id"), "left")
    .select(F.col("a.status"), F.col("at.lazada_count"), F.col("at.first_txn_time"))
    .orderBy("first_txn_time", "status")
)
