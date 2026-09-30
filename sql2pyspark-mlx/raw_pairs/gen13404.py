from pyspark.sql import functions as F

web_events_by_session = (
    df_web_events
    .filter(F.col("session_id") == 13)
    .groupBy("session_id")
    .agg(F.count("duration_sec").alias("duration_sec_count"))
)

result = (
    df_sessions.alias("s")
    .join(web_events_by_session.alias("webs"), F.col("s.id") == F.col("webs.session_id"), "inner")
    .filter(F.col("webs.duration_sec_count") >= 6)
    .select(F.col("s.browser"), F.col("webs.duration_sec_count"))
)
