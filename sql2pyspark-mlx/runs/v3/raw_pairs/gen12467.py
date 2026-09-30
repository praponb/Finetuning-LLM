from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("s.id") == F.col("we.session_id"), "right")
    .groupBy(F.col("s.device"))
    .agg(F.countDistinct(F.col("we.duration_sec")).alias("unique_event_types"))
    .orderBy(F.col("device").desc(), F.col("unique_event_types").desc())
    .limit(5)
)
