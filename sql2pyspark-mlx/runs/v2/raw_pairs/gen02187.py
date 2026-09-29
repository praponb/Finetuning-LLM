from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("s.id") == F.col("we.session_id"), "left")
    .filter(F.col("s.country") == "US")
    .select(F.col("we.session_id"), F.col("s.started_at"))
    .orderBy(F.col("session_id").desc())
)
