from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("merchant") == "7-Eleven")
    .groupBy("is_flagged")
    .agg(
        F.min("txn_time").alias("min_txn_time"),
        F.sum(F.when(F.col("merchant") == "7-Eleven", 1).otherwise(0)).alias("7_eleven_count"),
    )
    .filter(F.col("7_eleven_count") >= 43)
)
