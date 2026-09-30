from pyspark.sql import functions as F

result = (
    df_sessions
    .select(
        "id",
        "started_at",
        "country",
        F.datediff(F.current_date(), "started_at").alias("days_since_started_at"),
    )
)
