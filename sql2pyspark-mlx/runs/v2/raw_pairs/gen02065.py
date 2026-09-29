from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.hour("event_time").between(7, 14))
    .select(
        "id",
        "session_id",
        "page",
        "event_time",
        F.date_format("event_time", "yyyy-MM").alias("event_time_label"),
    )
)
