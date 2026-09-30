from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("merchant").isin(["Amazon", "7-Eleven", "Starbucks"]))
    .groupBy("txn_type")
    .agg(
        F.countDistinct("amount").alias("unique_merchants"),
        F.min("txn_time").alias("min_txn_time"),
    )
    .filter(F.col("unique_merchants") >= 7)
    .select("txn_type", "unique_merchants", "min_txn_time")
)
