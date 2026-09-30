from pyspark.sql import functions as F

result = (
    df_web_events
    .select(
        "session_id",
        "event_type",
        "page",
        F.col("event_time").cast("date").alias("event_time_day"),
    )
)
