from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("monthly_fee").between(90, 100))
    .select(
        "id",
        "end_date",
        "start_date",
        "customer_id",
        F.date_format("end_date", "yyyy-MM-dd").alias("end_date_label"),
        F.datediff("start_date", "end_date").alias("days_end_date_to_start_date"),
    )
)
