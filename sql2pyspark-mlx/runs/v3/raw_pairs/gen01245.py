from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("customer_id").isNotNull())
    .select(
        "id",
        "plan",
        "status",
        F.datediff(F.current_date(), "end_date").alias("days_since_end_date"),
    )
    .orderBy("status", "id")
    .limit(3)
)
