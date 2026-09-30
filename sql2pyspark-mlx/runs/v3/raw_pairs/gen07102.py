from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("is_flagged"))
    .groupBy("merchant")
    .agg(
        F.max("txn_time").alias("last_txn_time"),
        F.round(((100 * F.sum(F.when(F.col("merchant") == "7-Eleven", 1).otherwise(0))) / F.count("*")), 1).alias("pct_7_eleven"),
    )
    .filter(F.col("pct_7_eleven") < 71)
    .select("merchant", "last_txn_time", "pct_7_eleven")
    .orderBy("pct_7_eleven", "last_txn_time")
)
