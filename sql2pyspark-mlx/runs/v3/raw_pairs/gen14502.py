from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("txn_type").isin(["FEE", "CREDIT"]))
    .groupBy(
        "is_flagged",
        F.when(F.col("amount") >= 3400, "high").when(F.col("amount") >= 600, "mid").otherwise("low").alias("amount_bucket"),
    )
    .agg(
        F.min("txn_time").alias("first_txn_time"),
        (F.max("amount") - F.min("amount")).alias("amount_range"),
        F.round(F.avg("amount"), 2).alias("avg_amount"),
    )
)
