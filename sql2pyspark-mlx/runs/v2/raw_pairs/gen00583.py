from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy(
        F.when(F.col("amount") >= 3700, "high").when(F.col("amount") >= 1700, "mid").otherwise("low").alias("amount_bucket"),
    )
    .agg(
        F.avg("amount").alias("avg_amount"),
        F.round(((100 * F.sum(F.when(F.col("txn_type") == "REFUND", 1).otherwise(0))) / F.count("*")), 1).alias("pct_refund"),
        F.count("*").alias("cnt"),
    )
    .filter(F.col("cnt") >= 4)
    .drop("cnt")
)
