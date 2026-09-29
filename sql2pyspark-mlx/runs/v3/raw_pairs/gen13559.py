from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter((F.col("monthly_fee") <= 180) & (F.col("monthly_fee") <= 220) & F.col("start_date").between("2024-02-01", "2025-04-14"))
    .groupBy("plan")
    .agg(
        F.avg("monthly_fee").alias("avg_monthly_fee"),
        F.round(F.stddev("monthly_fee"), 2).alias("stddev_monthly_fee"),
    )
    .filter(F.col("stddev_monthly_fee") < 40)
)
