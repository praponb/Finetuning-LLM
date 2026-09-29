from pyspark.sql import functions as F

valid_subscriptions = (
    df_subscriptions
    .filter((F.year("start_date") >= 2023) & F.col("status").isin(["CANCELLED", "ACTIVE", "EXPIRED"]))
)

summary = (
    valid_subscriptions
    .groupBy(F.month("start_date").alias("start_month"))
    .agg(
        F.avg("monthly_fee").alias("avg_monthly_fee"),
        F.countDistinct("monthly_fee").alias("unique_customers"),
    )
)

result = (
    summary
    .filter(F.col("unique_customers") > 5)
    .select("start_month", "unique_customers")
    .orderBy("start_month")
)
