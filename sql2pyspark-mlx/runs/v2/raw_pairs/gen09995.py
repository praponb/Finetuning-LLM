from pyspark.sql import functions as F

src = (
    df_subscriptions
    .filter(F.col("monthly_fee") >= 230)
    .withColumn("monthly_fee_filled", F.coalesce("monthly_fee", F.lit(0)))
)

subscriptions_stats = (
    src
    .groupBy("status")
    .agg(F.sum("monthly_fee_filled").alias("total_monthly_fee_filled"))
)

result = (
    subscriptions_stats
    .select("status", "total_monthly_fee_filled")
    .orderBy(F.col("status").desc())
    .limit(1)
)
