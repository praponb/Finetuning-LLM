from pyspark.sql import functions as F

base = (
    df_payments
    .filter((F.col("paid_at") >= "2023-04-01") & (F.col("method").isNotNull() | F.col("amount").isNotNull()))
)

amount_bucket_summary = (
    base
    .groupBy(
        F.when(F.col("amount") >= 1540, "high").when(F.col("amount") >= 680, "mid").otherwise("low").alias("amount_bucket"),
    )
    .agg(
        F.min("amount").alias("min_amount"),
        F.round(F.stddev("amount"), 2).alias("stddev_amount"),
    )
)

result = amount_bucket_summary.filter(F.col("min_amount") > 820)
