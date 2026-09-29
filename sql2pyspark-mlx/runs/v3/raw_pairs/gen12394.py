from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy("is_flagged")
    .agg(
        F.max("txn_time").alias("last_txn_time"),
        F.round(F.avg("amount"), 2).alias("avg_amount"),
    )
    .orderBy(F.col("avg_amount").desc(), "is_flagged")
    .limit(50)
)
