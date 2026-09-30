from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("end_date").between("2023-08-01", "2023-10-26"))
    .groupBy("plan")
    .agg(
        F.min("monthly_fee").alias("min_monthly_fee"),
        F.round(((100 * F.sum(F.when(F.col("plan") == "enterprise", 1).otherwise(0))) / F.count("*")), 1).alias("pct_enterprise"),
    )
)
