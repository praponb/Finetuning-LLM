from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.hour("txn_time").between(8, 13) & (F.col("txn_time") >= "2025-10-01"))
    .groupBy(
        "is_flagged",
        F.when(F.col("amount") >= 3800, "high").when(F.col("amount") >= 1500, "mid").otherwise("low").alias("amount_bucket"),
    )
    .agg(
        F.round(F.stddev("amount"), 2).alias("stddev_amount"),
        F.countDistinct("amount").alias("unique_merchants"),
    )
    .filter(F.col("stddev_amount") >= 28)
    .orderBy(F.col("stddev_amount").desc())
)
