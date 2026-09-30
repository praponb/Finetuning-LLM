from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_web_events.alias("we"), F.col("s.id") == F.col("we.session_id"), "left")
    .filter(F.col("s.started_at") < "2025-03-01")
    .select(
        F.col("s.started_at"),
        F.col("s.device"),
        F.col("we.duration_sec"),
        F.col("we.event_time"),
    )
)
