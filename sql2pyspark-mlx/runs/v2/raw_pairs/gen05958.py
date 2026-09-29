from pyspark.sql import functions as F

web_events_by_session = (
    df_web_events
    .groupBy("session_id")
    .agg(
        F.round(F.avg("duration_sec"), 2).alias("avg_duration_sec"),
        F.count(F.when(F.col("event_type") == "page_view", 1)).alias("page_view_count"),
    )
)

result = (
    df_sessions.alias("s")
    .join(web_events_by_session.alias("webs"), F.col("s.id") == F.col("webs.session_id"), "inner")
    .filter(F.col("webs.avg_duration_sec") > 73)
    .select(
        F.col("s.id"),
        F.col("s.started_at"),
        F.col("webs.avg_duration_sec"),
        F.col("webs.page_view_count"),
    )
    .orderBy(F.col("id").desc(), "avg_duration_sec")
)
