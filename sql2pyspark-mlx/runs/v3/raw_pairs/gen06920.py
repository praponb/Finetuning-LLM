from pyspark.sql import functions as F

session_totals = (
    df_web_events
    .filter(F.col("event_type") == "page_view")
    .groupBy("session_id")
    .agg(
        F.max("duration_sec").alias("max_duration_sec"),
        F.min("event_time").alias("min_event_time"),
    )
)

result = (
    df_sessions.alias("s")
    .join(session_totals.alias("t"), F.col("s.id") == F.col("t.session_id"), "inner")
    .select(F.col("s.country"), F.col("t.max_duration_sec"), F.col("t.min_event_time"))
    .orderBy(F.col("min_event_time").desc())
)
