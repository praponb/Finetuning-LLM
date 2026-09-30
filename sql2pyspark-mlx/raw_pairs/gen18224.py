from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_web_events.alias("we"), F.col("we.session_id") == F.col("s.id"), "left")
    .filter(F.col("we.event_time") >= "2023-09-01")
    .groupBy(F.col("s.id"), F.col("s.country"))
    .agg(
        F.count(F.col("we.id")).alias("web_event_count"),
        F.sum(F.col("we.duration_sec")).alias("total_duration_sec"),
        F.countDistinct(F.col("we.duration_sec")).alias("unique_pages"),
    )
)
