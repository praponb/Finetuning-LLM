from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("we.session_id") == F.col("s.id"), "left")
    .select(F.col("we.event_type"), F.col("s.started_at"), F.col("s.country"))
)
