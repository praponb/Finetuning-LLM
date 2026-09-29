from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("amount") < 1600)
    .groupBy("is_flagged", "txn_type")
    .agg(
        F.min("txn_time").alias("min_txn_time"),
        F.max("amount").alias("max_amount"),
    )
    .filter(F.col("max_amount") > 1300)
    .select("is_flagged", "txn_type", "min_txn_time", "max_amount")
)
