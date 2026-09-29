from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter((F.col("monthly_fee") <= 170) & F.col("start_date").between("2023-09-01", "2023-09-18"))
    .agg(
        F.count("monthly_fee").alias("monthly_fee_count"),
        F.round(F.avg("monthly_fee"), 2).alias("avg_monthly_fee"),
        F.max("start_date").alias("latest_start_date"),
    )
)
