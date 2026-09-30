from pyspark.sql import functions as F

result = (
    df_web_events
    .select(
        "id",
        "page",
        "event_time",
        "session_id",
        F.to_date("event_time").alias("event_time_date"),
        F.to_date("event_time").alias("event_time_date_2"),
    )
)
