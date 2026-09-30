from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("we.session_id") == F.col("s.id"), "left")
    .filter(F.hour(F.col("we.event_time")).between(7, 11))
    .groupBy(F.col("s.browser"))
    .agg(F.min(F.col("we.duration_sec")).alias("min_duration_sec"))
    .orderBy("min_duration_sec")
)
