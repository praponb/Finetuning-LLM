from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy(
        F.when(F.col("balance") >= 85000, "high").when(F.col("balance") >= 11000, "mid").otherwise("low").alias("balance_bucket"),
        "status",
    )
    .agg(
        F.round(F.avg("balance"), 2).alias("avg_balance"),
        F.round(F.stddev("balance"), 2).alias("stddev_balance"),
        F.min("balance").alias("min_balance"),
    )
    .filter(F.col("avg_balance") < 33)
    .orderBy(F.col("stddev_balance").desc())
)
