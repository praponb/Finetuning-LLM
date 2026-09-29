from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("we.session_id") == F.col("s.id"), "inner")
    .filter(F.col("we.duration_sec") == 199)
    .select(F.col("we.duration_sec"), F.col("s.device"), F.col("s.started_at"))
)
