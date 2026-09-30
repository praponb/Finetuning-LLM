from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("amount") > 3000)
    .groupBy("is_flagged", F.month("txn_time").alias("txn_month"))
    .agg(
        F.count("*").alias("row_count"),
        F.avg("amount").alias("avg_amount"),
    )
)
