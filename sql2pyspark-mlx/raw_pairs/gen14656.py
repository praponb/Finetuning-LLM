from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("we.session_id") == F.col("s.id"), "inner")
    .filter(F.hour(F.col("s.started_at")).between(11, 14) & F.col("s.browser").isin(["Edge", "Safari", "Chrome"]))
    .select(F.col("we.id"), F.col("we.duration_sec"), F.col("s.device"))
)
