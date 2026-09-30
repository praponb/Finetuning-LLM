from pyspark.sql import functions as F

web_events_by_session = (
    df_web_events
    .groupBy("session_id")
    .agg(F.min("duration_sec").alias("min_duration_sec"))
)

result = (
    df_sessions.alias("s")
    .join(web_events_by_session.alias("webs"), F.col("s.id") == F.col("webs.session_id"), "inner")
    .filter(F.col("webs.min_duration_sec") > 450)
    .select(F.col("s.id"), F.col("s.device"), F.col("webs.min_duration_sec"))
    .orderBy("device", "id")
    .limit(20)
)
