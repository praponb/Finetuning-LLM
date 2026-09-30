from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("we.session_id") == F.col("s.id"), "left")
    .filter(F.to_date(F.col("s.started_at")) < "2025-09-04")
    .select(
        F.col("we.session_id"),
        F.col("we.duration_sec"),
        F.col("s.browser"),
        F.col("s.device"),
    )
)
