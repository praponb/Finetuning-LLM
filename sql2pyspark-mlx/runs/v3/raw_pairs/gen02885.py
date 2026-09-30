from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_web_events.alias("we"), F.col("we.session_id") == F.col("s.id"), "left")
    .groupBy(F.col("s.id"), F.col("s.device"))
    .agg(
        F.count(F.col("we.id")).alias("web_event_count"),
        F.min(F.col("we.event_time")).alias("first_event_time"),
        F.countDistinct(F.col("we.duration_sec")).alias("unique_event_types"),
    )
    .orderBy("device")
)
