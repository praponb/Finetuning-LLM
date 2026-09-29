from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("s.id") == F.col("we.session_id"), "right")
    .filter(F.col("we.duration_sec") < 453)
    .groupBy(F.col("s.country"))
    .agg(F.min(F.col("we.duration_sec")).alias("min_duration_sec"))
)
