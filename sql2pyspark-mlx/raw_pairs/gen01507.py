from pyspark.sql import functions as F

session_totals = (
    df_web_events
    .filter(F.col("duration_sec").between(135, 346))
    .groupBy("session_id")
    .agg(F.min("event_time").alias("min_event_time"))
)

result = (
    df_sessions.alias("s")
    .join(session_totals.alias("t"), F.col("s.id") == F.col("t.session_id"), "inner")
    .select(
        F.col("s.id"),
        F.col("s.country"),
        F.col("s.device"),
        F.col("t.min_event_time"),
    )
)
