from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("we.session_id") == F.col("s.id"), "left")
    .select(
        F.col("we.id"),
        F.col("we.duration_sec"),
        F.col("s.browser"),
        F.col("s.started_at"),
        F.date_format(F.col("we.event_time"), "yyyy-MM").alias("event_time_label"),
    )
)
