from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("we.session_id") == F.col("s.id"), "left")
    .filter(F.col("we.duration_sec").isNull() & F.hour(F.col("we.event_time")).between(11, 19))
    .select(F.col("we.page"), F.col("s.country"))
)
