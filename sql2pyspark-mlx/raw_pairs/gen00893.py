from pyspark.sql import functions as F

result = (
    df_sessions
    .select(
        "country",
        F.datediff(F.current_date(), "started_at").alias("days_since_started_at"),
    )
    .orderBy("days_since_started_at")
)
