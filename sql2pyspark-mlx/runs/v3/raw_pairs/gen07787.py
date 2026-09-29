from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_web_events.alias("we"), F.col("s.id") == F.col("we.session_id"), "inner")
    .filter(F.col("s.started_at") >= "2023-11-08")
    .select(
        F.col("s.started_at"),
        F.col("s.device"),
        F.col("s.browser"),
        F.col("we.event_type"),
        F.col("we.duration_sec"),
    )
    .orderBy("event_type")
)
