from pyspark.sql import functions as F

src = (
    df_subscriptions
    .filter((F.year("start_date") == 2024) & F.col("end_date").between("2024-05-15", "2025-07-24"))
)

status_summary = (
    src
    .groupBy("status")
    .agg(
        F.round(F.avg("monthly_fee"), 2).alias("avg_monthly_fee"),
        F.min("monthly_fee").alias("min_monthly_fee"),
    )
)

result = (
    status_summary
    .filter(F.col("avg_monthly_fee") > 44)
    .select("status", "avg_monthly_fee", "min_monthly_fee")
    .orderBy(F.col("avg_monthly_fee").desc(), F.col("min_monthly_fee").desc())
)
