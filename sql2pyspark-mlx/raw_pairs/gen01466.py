from pyspark.sql import functions as F

result = (
    df_subscriptions
    .select(
        "customer_id",
        "plan",
        F.datediff("start_date", "end_date").alias("days_end_date_to_start_date"),
        (F.datediff(F.current_date(), "end_date") / 365.25).alias("years_since_end_date"),
    )
    .orderBy(F.col("days_end_date_to_start_date").desc())
)
