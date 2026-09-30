from pyspark.sql import functions as F

src = (
    df_subscriptions
    .filter((F.col("customer_id") == 39) & (F.col("customer_id") == 13) & F.month("end_date").isin([5, 7, 8]))
)

plan_summary = (
    src
    .groupBy("plan")
    .agg(
        F.min("start_date").alias("first_start_date"),
        F.sum("monthly_fee").alias("total_monthly_fee"),
    )
)

result = (
    plan_summary
    .orderBy(F.col("first_start_date").desc(), "plan")
    .limit(5)
)
