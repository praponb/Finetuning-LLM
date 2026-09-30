from pyspark.sql import functions as F

result = (
    df_web_events
    .select(
        "event_type",
        "event_time",
        F.datediff(F.current_date(), "event_time").alias("days_since_event_time"),
    )
)
