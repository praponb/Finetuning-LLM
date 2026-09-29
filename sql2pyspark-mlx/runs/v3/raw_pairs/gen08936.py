from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("amount") > 1380)
    .groupBy(
        F.when(F.col("amount") >= 1530, "high").when(F.col("amount") >= 440, "mid").otherwise("low").alias("amount_bucket"),
        "status",
    )
    .agg(
        F.countDistinct("amount").alias("unique_methods"),
        F.round(F.stddev("amount"), 2).alias("stddev_amount"),
    )
    .orderBy(F.col("unique_methods").desc())
)
