from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("event_type") == "purchase")
    .select(
        "id",
        "event_time",
        "event_type",
        "duration_sec",
        F.datediff(F.current_date(), "event_time").alias("days_since_event_time"),
        F.trim("event_type").alias("clean_event_type"),
    )
)
