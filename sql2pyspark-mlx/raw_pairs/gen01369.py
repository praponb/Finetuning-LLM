from pyspark.sql import functions as F

result = (
    df_web_events
    .filter((F.col("event_time") < "2023-11-27") & (F.to_date("event_time") >= "2025-01-01"))
    .select(
        "id",
        "event_time",
        "session_id",
        F.datediff(F.current_date(), "event_time").alias("days_since_event_time"),
    )
)
