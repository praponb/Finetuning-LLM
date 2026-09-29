from pyspark.sql import functions as F

recent_subscriptions = df_subscriptions.filter(F.col("monthly_fee") >= 220)

status_summary = (
    recent_subscriptions
    .groupBy("status")
    .agg(
        F.count("monthly_fee").alias("monthly_fee_count"),
        F.countDistinct("monthly_fee").alias("unique_statuss"),
    )
)

result = (
    status_summary
    .filter(F.col("monthly_fee_count") > 6)
    .select("status", "monthly_fee_count", "unique_statuss")
)
