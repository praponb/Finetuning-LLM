from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("we.session_id") == F.col("s.id"), "left")
    .filter(F.col("s.started_at") < "2023-08-01")
    .groupBy(F.col("s.country"), F.col("s.device"))
    .agg(F.min(F.col("we.duration_sec")).alias("min_duration_sec"))
)
