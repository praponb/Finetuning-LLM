from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("s.id") == F.col("we.session_id"), "inner")
    .filter((F.col("s.started_at") >= "2025-08-09") & (F.col("we.page") == "/checkout"))
    .select(F.col("we.duration_sec"), F.col("we.page"), F.col("s.device"))
)
