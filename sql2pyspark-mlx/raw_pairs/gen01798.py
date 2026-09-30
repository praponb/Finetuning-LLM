from pyspark.sql import functions as F

recent_transactions = (
    df_transactions
    .filter(F.col("txn_type") != "DEBIT")
    .withColumn("is_flagged_flag", F.col("is_flagged").cast("int"))
)

agg = (
    recent_transactions
    .groupBy("txn_type")
    .agg(
        F.max("txn_time").alias("latest_txn_time"),
        F.round(((100 * F.sum(F.when(F.col("merchant") == "Grab", 1).otherwise(0))) / F.count("*")), 1).alias("pct_grab"),
    )
)

result = agg.orderBy("latest_txn_time", F.col("txn_type").desc())
