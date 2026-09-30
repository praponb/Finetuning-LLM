from pyspark.sql import functions as F

result = (
    df_web_events
    .select(
        "id",
        "event_time",
        F.datediff(F.current_date(), "event_time").alias("days_since_event_time"),
        F.date_format("event_time", "yyyy-MM").alias("event_time_label"),
    )
)
