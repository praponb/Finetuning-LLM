from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("customer_id").isNull() & (F.col("monthly_fee") <= 190))
    .select(
        "id",
        "monthly_fee",
        "plan",
        F.date_trunc("WEEK", "end_date").alias("end_date_period"),
        F.concat_ws(",", "status", "plan").alias("status_plan_key"),
    )
    .orderBy(F.col("monthly_fee").desc())
)
