from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("merchant").isin(["Shell", "7-Eleven"]))
    .groupBy(
        F.when(F.col("amount") >= 3100, "high").when(F.col("amount") >= 900, "mid").otherwise("low").alias("amount_bucket"),
        F.when(F.year("txn_time") < 2024, "before_2024").when(F.year("txn_time") == 2024, "2024").otherwise("after_2024").alias("txn_cohort"),
    )
    .agg(F.round(F.stddev("amount"), 2).alias("stddev_amount"))
)
