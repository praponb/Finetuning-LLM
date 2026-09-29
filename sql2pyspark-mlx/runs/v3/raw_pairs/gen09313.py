from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("we.session_id") == F.col("s.id"), "inner")
    .groupBy(F.col("s.device"))
    .agg(F.min(F.col("we.event_time")).alias("min_event_time"))
    .orderBy(F.col("min_event_time").desc(), F.col("device").desc())
    .limit(1)
)
