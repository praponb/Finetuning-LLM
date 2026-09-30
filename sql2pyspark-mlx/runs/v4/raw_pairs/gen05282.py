from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("monthly_fee") <= 100)
    .select(
        "start_date",
        "monthly_fee",
        F.datediff(F.current_date(), "start_date").alias("days_since_start_date"),
        F.date_format("start_date", "yyyy-MM-dd").alias("start_date_label"),
    )
    .orderBy(F.col("monthly_fee").desc())
)
