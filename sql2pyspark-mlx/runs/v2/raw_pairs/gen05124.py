from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy(F.year("txn_time").alias("txn_year"), "merchant")
    .agg(
        F.avg("amount").alias("avg_amount"),
        F.count("*").alias("total_count"),
    )
    .filter(F.col("total_count") >= 6)
    .orderBy("total_count", "txn_year", "merchant")
    .limit(1)
)
