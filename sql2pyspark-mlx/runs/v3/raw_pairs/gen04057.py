from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy(F.month("txn_time").alias("txn_month"))
    .agg(
        F.count("amount").alias("amount_count"),
        F.round(F.avg("amount"), 2).alias("avg_amount"),
    )
    .filter(F.col("amount_count") > 7)
    .orderBy(F.col("txn_month").desc())
)
