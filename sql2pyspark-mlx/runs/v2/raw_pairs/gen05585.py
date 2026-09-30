from pyspark.sql import functions as F

result = (
    df_web_events
    .select(
        "id",
        "event_time",
        "duration_sec",
        "page",
        F.to_date("event_time").alias("event_time_date"),
    )
)
