from pyspark.sql import functions as F

web_events_by_session = (
    df_web_events
    .filter(F.hour("event_time").between(11, 18))
    .groupBy("session_id")
    .agg(F.max("event_time").alias("latest_event_time"))
)

result = (
    df_sessions.alias("s")
    .join(web_events_by_session.alias("t"), F.col("s.id") == F.col("t.session_id"), "inner")
    .select(F.col("s.browser"), F.col("t.latest_event_time"))
)
