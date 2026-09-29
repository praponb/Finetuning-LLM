from pyspark.sql import functions as F

session_totals = (
    df_web_events
    .groupBy("session_id")
    .agg(F.countDistinct("duration_sec").alias("unique_event_types"))
)

result = (
    df_sessions.alias("s")
    .join(session_totals.alias("t"), F.col("s.id") == F.col("t.session_id"), "left")
    .filter(F.col("t.unique_event_types") >= 7)
    .select(F.col("s.browser"), F.col("s.started_at"), F.col("t.unique_event_types"))
)
