from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("s.id") == F.col("we.session_id"), "left")
    .select(
        F.col("we.id"),
        F.col("we.event_time"),
        F.col("we.event_type"),
        F.col("s.device"),
        F.col("s.started_at"),
    )
    .orderBy("event_time")
)
