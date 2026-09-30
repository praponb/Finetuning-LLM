from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("we.session_id") == F.col("s.id"), "inner")
    .filter(F.col("we.event_type") == "click")
    .groupBy(F.col("s.device"))
    .agg(F.countDistinct(F.col("we.duration_sec")).alias("unique_pages"))
    .orderBy(F.col("device").desc())
)
