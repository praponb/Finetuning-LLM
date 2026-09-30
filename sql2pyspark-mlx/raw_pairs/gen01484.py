from pyspark.sql import functions as F

web_events_by_session = (
    df_web_events
    .groupBy("session_id")
    .agg(F.round(F.avg("duration_sec"), 2).alias("avg_duration_sec"))
)

result = (
    df_sessions.alias("s")
    .join(web_events_by_session.alias("webs"), F.col("s.id") == F.col("webs.session_id"), "inner")
    .filter(F.col("webs.avg_duration_sec") >= 77)
    .select(F.col("s.id"), F.col("s.device"), F.col("webs.avg_duration_sec"))
    .orderBy(F.col("avg_duration_sec").desc(), "id")
    .limit(3)
)
