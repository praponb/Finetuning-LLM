from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy(F.hour("txn_time").alias("txn_hour"), "merchant", "txn_type")
    .agg(
        F.avg("amount").alias("avg_amount"),
        F.max("amount").alias("max_amount"),
    )
    .orderBy(F.col("merchant").desc(), "txn_type", "txn_hour")
    .limit(3)
)
