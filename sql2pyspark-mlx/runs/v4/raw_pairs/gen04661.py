from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("customer_id").isNotNull())
    .select(
        "status",
        F.datediff("end_date", "start_date").alias("days_start_date_to_end_date"),
        F.date_format("start_date", "MMM yyyy").alias("start_date_label"),
    )
)
