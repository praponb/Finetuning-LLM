from pyspark.sql import functions as F

src = (
    df_subscriptions
    .filter(((F.col("status") != "CANCELLED") & (F.year("start_date") >= 2023)) | (F.col("plan") != "enterprise"))
)

subscriptions_stats = (
    src
    .groupBy("status")
    .agg(
        F.round(F.avg("monthly_fee"), 2).alias("avg_monthly_fee"),
        F.min("end_date").alias("first_end_date"),
    )
)

result = (
    subscriptions_stats
    .select("status", "avg_monthly_fee", "first_end_date")
    .orderBy(F.col("avg_monthly_fee").desc())
)
