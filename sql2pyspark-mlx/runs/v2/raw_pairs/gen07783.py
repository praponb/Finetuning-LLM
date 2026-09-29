from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter((F.col("monthly_fee") <= 120) & F.col("end_date").between("2023-08-26", "2025-04-29"))
    .groupBy("status")
    .agg(
        F.countDistinct("monthly_fee").alias("unique_plans"),
        F.sum("monthly_fee").alias("total_monthly_fee"),
        F.round(F.stddev("monthly_fee"), 2).alias("stddev_monthly_fee"),
    )
    .orderBy(F.col("total_monthly_fee").desc(), "status")
)
