from pyspark.sql import functions as F

web_events_by_session = (
    df_web_events
    .groupBy("session_id")
    .agg(
        F.min("event_time").alias("min_event_time"),
        F.sum(F.when(F.col("page") == "/search", F.col("duration_sec")).otherwise(0)).alias("search_duration_sec"),
    )
)

result = (
    df_sessions.alias("s")
    .join(web_events_by_session.alias("t"), F.col("s.id") == F.col("t.session_id"), "inner")
    .filter(F.col("t.search_duration_sec") >= 75)
    .select(
        F.col("s.id"),
        F.col("s.device"),
        F.col("s.country"),
        F.col("t.min_event_time"),
        F.col("t.search_duration_sec"),
    )
    .orderBy(F.col("min_event_time").desc(), "id")
    .limit(3)
)
