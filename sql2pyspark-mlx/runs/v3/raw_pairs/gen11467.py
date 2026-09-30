from pyspark.sql import functions as F

result = (
    df_subscriptions
    .select(
        "status",
        F.trim("status").alias("clean_status"),
        F.datediff(F.current_date(), "start_date").alias("days_since_start_date"),
    )
    .orderBy(F.col("days_since_start_date").desc(), "status")
)
