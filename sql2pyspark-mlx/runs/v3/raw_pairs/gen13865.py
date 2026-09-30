from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_web_events.alias("we"), F.col("we.session_id") == F.col("s.id"), "left")
    .filter(F.col("we.session_id").isin([8, 40]) & (F.col("we.event_time") < "2024-01-01"))
    .select(
        F.col("s.browser"),
        F.col("s.started_at"),
        F.col("s.country"),
        F.col("we.duration_sec"),
        F.col("we.session_id"),
    )
    .orderBy("session_id", F.col("country").desc())
)
