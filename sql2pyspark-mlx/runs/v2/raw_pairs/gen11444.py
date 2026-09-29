from pyspark.sql import functions as F

result = (
    df_web_events
    .select(
        "id",
        "duration_sec",
        "event_type",
        "session_id",
        F.date_format("event_time", "yyyy-MM-dd").alias("event_time_label"),
        F.trim("event_type").alias("clean_event_type"),
    )
)
