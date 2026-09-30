from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("monthly_fee").between(130, 160) & F.col("plan").isin(["premium", "basic"]) & F.col("customer_id").isNull())
    .select(
        "id",
        "start_date",
        "monthly_fee",
        "plan",
        F.last_day("end_date").alias("end_date_month_end"),
    )
)
