from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy(
        F.when(F.col("amount") >= 1640, "high").when(F.col("amount") >= 510, "mid").otherwise("low").alias("amount_bucket"),
    )
    .agg(
        F.round(F.avg("amount"), 2).alias("avg_amount"),
        F.round(F.stddev("amount"), 2).alias("stddev_amount"),
    )
    .filter(F.col("stddev_amount") > 27)
)
