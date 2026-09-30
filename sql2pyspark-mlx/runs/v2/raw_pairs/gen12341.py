from pyspark.sql import functions as F

result = (
    df_transactions
    .filter((F.col("merchant") == "Starbucks") & (F.col("txn_time") < "2025-02-01"))
    .rollup(F.month("txn_time").alias("txn_month"), "account_id")
    .agg(
        F.sum("amount").alias("total_amount"),
        F.max("txn_time").alias("latest_txn_time"),
    )
    .orderBy("total_amount")
)
