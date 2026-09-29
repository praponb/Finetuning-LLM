from pyspark.sql import functions as F

recent_subscriptions = (
    df_subscriptions
    .filter(F.col("monthly_fee") < 70)
    .withColumn("start_date_label", F.date_format("start_date", "MMM yyyy"))
)

agg = (
    recent_subscriptions
    .groupBy("plan")
    .agg(
        F.min("monthly_fee").alias("min_monthly_fee"),
        F.sum(F.when(F.col("status") == "PAUSED", F.col("monthly_fee")).otherwise(0)).alias("paused_monthly_fee"),
    )
)

result = (
    agg
    .filter(F.col("paused_monthly_fee") < 21)
    .orderBy("paused_monthly_fee", F.col("min_monthly_fee").desc())
)
