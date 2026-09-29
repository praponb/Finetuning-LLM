from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter((F.col("monthly_fee") < 180) | F.col("start_date").between("2023-05-01", "2024-10-01"))
    .groupBy("status", F.date_format("end_date", "yyyy-MM").alias("end_month"))
    .agg(
        F.count("*").alias("num_subscriptions"),
        F.avg("monthly_fee").alias("avg_monthly_fee"),
    )
    .filter(F.col("num_subscriptions") < 4)
    .orderBy(F.col("avg_monthly_fee").desc())
)
