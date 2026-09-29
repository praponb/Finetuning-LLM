from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("s.id") == F.col("we.session_id"), "left")
    .groupBy(F.col("s.country"))
    .agg(F.min(F.col("we.duration_sec")).alias("min_duration_sec"))
    .orderBy("country")
)
