from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("is_flagged") & F.col("merchant").isin(["Lazada", "7-Eleven"]))
    .groupBy(
        F.when(F.col("amount") >= 3600, "high").when(F.col("amount") >= 1300, "mid").otherwise("low").alias("amount_bucket"),
        "txn_type",
    )
    .agg(
        F.max("txn_time").alias("latest_txn_time"),
        F.round(F.avg("amount"), 2).alias("avg_amount"),
    )
    .filter(F.col("avg_amount") >= 20)
)
