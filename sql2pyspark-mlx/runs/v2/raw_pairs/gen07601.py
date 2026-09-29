from pyspark.sql import functions as F

web_events_by_session = (
    df_web_events
    .groupBy("session_id")
    .agg(F.countDistinct("duration_sec").alias("unique_event_types"))
)

result = (
    df_sessions.alias("s")
    .join(web_events_by_session.alias("W"), F.col("s.id") == F.col("W.session_id"), "left")
    .select(F.col("s.country"), F.col("s.started_at"), F.col("W.unique_event_types"))
)
