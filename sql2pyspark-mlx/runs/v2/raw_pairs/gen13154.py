from pyspark.sql import functions as F

web_events_by_session = (
    df_web_events
    .filter(F.hour("event_time").between(7, 14))
    .groupBy("session_id")
    .agg(F.max("event_time").alias("latest_event_time"))
)

result = (
    df_sessions.alias("s")
    .join(web_events_by_session.alias("W"), F.col("s.id") == F.col("W.session_id"), "left")
    .select(F.col("s.id"), F.col("s.country"), F.col("W.latest_event_time"))
)
