from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_web_events.filter(F.to_date("event_time") >= "2025-10-16").alias("we"), F.col("s.id") == F.col("we.session_id"), "left_anti")
    .select(F.col("s.id"), F.col("s.device"), F.col("s.browser"), F.col("s.started_at"))
)
