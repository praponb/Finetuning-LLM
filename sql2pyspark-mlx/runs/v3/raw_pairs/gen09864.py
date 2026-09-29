from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("customer_id").isNotNull())
    .select(
        "end_date",
        "status",
        F.initcap(F.trim("status")).alias("status_title"),
        F.datediff(F.current_date(), "end_date").alias("days_since_end_date"),
    )
)
