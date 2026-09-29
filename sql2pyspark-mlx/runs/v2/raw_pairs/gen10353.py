from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_web_events.alias("we"), F.col("s.id") == F.col("we.session_id"), "left")
    .filter(F.col("we.duration_sec").isNull() & F.col("we.page").isNull())
    .select(
        F.col("s.browser"),
        F.col("s.started_at"),
        F.col("s.device"),
        F.col("we.duration_sec"),
        F.col("we.event_type"),
    )
    .orderBy("device")
)
