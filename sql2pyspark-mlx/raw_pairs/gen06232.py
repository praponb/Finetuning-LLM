from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy(F.date_trunc("YEAR", "start_date").alias("year_start"))
    .agg(
        F.count(F.when(F.col("plan") == "premium", 1)).alias("premium_count"),
        F.round(F.avg("monthly_fee"), 2).alias("avg_monthly_fee"),
        F.count(F.when(F.col("plan") == "enterprise", 1)).alias("enterprise_count"),
    )
    .orderBy(F.col("year_start").desc())
)
