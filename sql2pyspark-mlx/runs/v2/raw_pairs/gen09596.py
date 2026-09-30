from pyspark.sql import functions as F

filtered_transactions = (
    df_transactions
    .filter((F.col("txn_type") == "CREDIT") & F.col("account_id").isin([3, 12, 34, 37]))
)

txn_hour_summary = (
    filtered_transactions
    .groupBy(F.hour("txn_time").alias("txn_hour"))
    .agg(
        F.sum(F.when(F.col("merchant") == "Shell", 1).otherwise(0)).alias("shell_count"),
        F.min("txn_time").alias("min_txn_time"),
    )
)

result = (
    txn_hour_summary
    .filter(F.col("shell_count") >= 24)
    .select("txn_hour", "shell_count", "min_txn_time")
    .orderBy(F.col("min_txn_time").desc(), "txn_hour")
    .limit(3)
)
