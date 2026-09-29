from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy(
        "merchant",
        F.when(F.col("amount") >= 3700, "high").when(F.col("amount") >= 1400, "mid").otherwise("low").alias("amount_bucket"),
        F.hour("txn_time").alias("txn_hour"),
    )
    .agg(
        (F.max("amount") - F.min("amount")).alias("amount_range"),
        F.round(F.stddev("amount"), 2).alias("stddev_amount"),
        F.min("amount").alias("min_amount"),
    )
    .orderBy(F.col("min_amount").desc())
)
