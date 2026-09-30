from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("merchant").isin(["Shell", "Starbucks", "Amazon"]))
    .groupBy(
        "merchant",
        F.when(F.col("amount") >= 4100, "high").when(F.col("amount") >= 1100, "mid").otherwise("low").alias("amount_bucket"),
    )
    .agg(
        F.round(F.avg("amount"), 2).alias("avg_amount"),
        F.min("txn_time").alias("first_txn_time"),
    )
    .filter(F.col("avg_amount") < 32)
    .orderBy("first_txn_time", "avg_amount")
)
