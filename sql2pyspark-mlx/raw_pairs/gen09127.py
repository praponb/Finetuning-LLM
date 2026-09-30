from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "outer")
    .groupBy(F.col("a.account_type"))
    .agg(
        F.max(F.col("t.txn_time")).alias("latest_txn_time"),
        F.sum(F.when(F.col("t.merchant") == "Grab", F.col("t.amount")).otherwise(0)).alias("grab_amount"),
    )
    .orderBy("account_type")
)
