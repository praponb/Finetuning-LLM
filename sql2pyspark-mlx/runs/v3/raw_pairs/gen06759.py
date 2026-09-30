from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("txn_type").isin(["REFUND", "FEE", "DEBIT"]))
    .groupBy("txn_type", "merchant")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("merchant") == "7-Eleven", 1).otherwise(0))) / F.count("*")), 1).alias("pct_7_eleven"),
        F.min("txn_time").alias("min_txn_time"),
    )
    .filter(F.col("pct_7_eleven") >= 77)
    .orderBy("txn_type")
)
