from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("we.session_id") == F.col("s.id"), "inner")
    .groupBy(F.col("s.device"))
    .agg(
        F.countDistinct(F.col("we.duration_sec")).alias("unique_event_types"),
        F.avg(F.col("we.duration_sec")).alias("avg_duration_sec"),
    )
)
