from pyspark.sql import functions as F

result = (
    df_sessions
    .select(
        "id",
        "device",
        "customer_id",
        F.when(F.col("browser") == "Chrome", 1).otherwise(0).alias("is_chrome"),
        F.datediff(F.current_date(), "started_at").alias("days_since_started_at"),
    )
)
