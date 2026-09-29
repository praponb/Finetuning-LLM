from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("we.session_id") == F.col("s.id"), "inner")
    .filter(F.col("we.duration_sec") >= 194)
    .select(
        F.col("we.session_id"),
        F.col("s.customer_id"),
        F.col("s.country"),
        F.col("s.started_at"),
        F.col("we.event_time").cast("date").alias("event_time_day"),
    )
    .orderBy(F.col("session_id").desc())
)
