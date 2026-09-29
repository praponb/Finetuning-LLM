from pyspark.sql import functions as F

base = df_subscriptions.filter(F.year("start_date") == 2025)

summary = (
    base
    .groupBy("plan")
    .agg(
        F.countDistinct("monthly_fee").alias("unique_plans"),
        F.count("monthly_fee").alias("monthly_fee_count"),
    )
)

result = (
    summary
    .filter(F.col("unique_plans") >= 7)
    .orderBy("unique_plans")
)
