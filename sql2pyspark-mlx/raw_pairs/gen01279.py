from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_web_events.alias("we"), F.col("s.id") == F.col("we.session_id"), "left")
    .filter((F.col("s.country") != "US") & (F.col("we.session_id") == 6))
    .select(
        F.col("s.started_at"),
        F.col("we.duration_sec"),
        F.hour(F.col("we.event_time")).alias("event_time_hour"),
    )
)
