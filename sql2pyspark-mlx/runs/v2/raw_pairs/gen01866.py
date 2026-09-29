from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_web_events.alias("we"), F.col("we.session_id") == F.col("s.id"), "left")
    .filter((F.col("we.duration_sec") >= 475) & F.hour(F.col("s.started_at")).between(7, 13))
    .select(
        F.col("s.started_at"),
        F.col("we.event_type"),
        F.col("we.session_id"),
        F.col("we.duration_sec"),
    )
    .orderBy("event_type")
)
