from pyspark.sql import functions as F

web_events_by_session = (
    df_web_events
    .filter(F.col("session_id").isin([5, 21, 22, 38]))
    .groupBy("session_id")
    .agg(F.sum("duration_sec").alias("total_duration_sec"))
)

result = (
    df_sessions.alias("s")
    .join(web_events_by_session.alias("webs"), F.col("s.id") == F.col("webs.session_id"), "inner")
    .filter(F.col("webs.total_duration_sec") > 1600)
    .select(
        F.col("s.id"),
        F.col("s.device"),
        F.col("s.started_at"),
        F.col("webs.total_duration_sec"),
    )
)
