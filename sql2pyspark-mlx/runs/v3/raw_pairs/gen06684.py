from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("is_flagged") & (F.col("merchant") == "Starbucks") & F.hour("txn_time").between(6, 12))
    .groupBy(F.hour("txn_time").alias("txn_hour"))
    .agg(
        F.round(F.avg("amount"), 2).alias("avg_amount"),
        F.sum(F.when(F.col("merchant") == "Amazon", 1).otherwise(0)).alias("amazon_count"),
    )
    .orderBy(F.col("txn_hour").desc())
)
