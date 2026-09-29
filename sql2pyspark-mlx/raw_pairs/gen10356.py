from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("end_date") >= "2023-05-01")
    .groupBy("status")
    .agg(
        F.round(F.avg("monthly_fee"), 2).alias("avg_monthly_fee"),
        F.countDistinct("monthly_fee").alias("unique_customers"),
    )
    .filter(F.col("unique_customers") > 6)
    .select("status", "avg_monthly_fee", "unique_customers")
    .orderBy("status")
)
