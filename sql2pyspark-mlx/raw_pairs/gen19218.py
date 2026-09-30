from pyspark.sql import functions as F

session_totals = (
    df_web_events
    .groupBy("session_id")
    .agg(F.min("event_time").alias("min_event_time"))
)

result = (
    df_sessions.alias("s")
    .join(session_totals.alias("S"), F.col("s.id") == F.col("S.session_id"), "left")
    .select(F.col("s.country"), F.col("S.min_event_time"))
    .orderBy("min_event_time")
)
