from pyspark.sql import functions as F

src = (
    df_subscriptions
    .filter(F.col("customer_id").isin([9, 28, 29, 32]))
    .withColumn("monthly_fee_rounded", F.round("monthly_fee", 1))
)

subscriptions_stats = (
    src
    .groupBy("customer_id")
    .agg(
        F.avg("monthly_fee").alias("avg_monthly_fee"),
        F.min("monthly_fee_rounded").alias("min_monthly_fee_rounded"),
    )
)

result = subscriptions_stats.orderBy(F.col("customer_id").desc())
