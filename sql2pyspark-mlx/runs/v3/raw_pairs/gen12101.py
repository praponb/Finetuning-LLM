from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("is_flagged") & (F.col("txn_time") < "2024-07-01") & (F.col("txn_type") == "REFUND"))
    .agg(
        F.max("txn_time").alias("latest_txn_time"),
        F.count("amount").alias("amount_count"),
    )
    .orderBy(F.col("latest_txn_time").desc())
)
