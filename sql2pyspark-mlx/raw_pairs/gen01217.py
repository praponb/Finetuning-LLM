from pyspark.sql import functions as F

valid_subscriptions = (
    df_subscriptions
    .filter(~(F.col("plan").isin(["premium", "enterprise"]) & (F.year("end_date") >= 2024)))
)

start_cohort_summary = (
    valid_subscriptions
    .groupBy(
        F.when(F.year("start_date") < 2024, "before_2024").when(F.year("start_date") == 2024, "2024").otherwise("after_2024").alias("start_cohort"),
    )
    .agg(F.sum("monthly_fee").alias("total_monthly_fee"))
)

result = (
    start_cohort_summary
    .select("start_cohort", "total_monthly_fee")
    .orderBy(F.col("total_monthly_fee").desc())
)
