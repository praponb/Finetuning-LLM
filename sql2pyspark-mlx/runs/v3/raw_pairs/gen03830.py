from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("a.id") == F.col("t.account_id"), "left")
    .groupBy(F.col("a.status"))
    .agg(
        F.sum(F.when(F.col("t.txn_type") == "REFUND", 1).otherwise(0)).alias("refund_count"),
        F.count("*").alias("num_transactions"),
    )
)
