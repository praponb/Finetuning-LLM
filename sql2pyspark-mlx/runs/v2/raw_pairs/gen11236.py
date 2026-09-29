from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_web_events.alias("we"), F.col("we.session_id") == F.col("s.id"), "inner")
    .groupBy(F.col("s.country"))
    .agg(
        F.count(F.col("we.id")).alias("web_event_count"),
        (F.max(F.col("we.duration_sec")) - F.min(F.col("we.duration_sec"))).alias("duration_sec_range"),
        F.max(F.col("we.event_time")).alias("last_event_time"),
    )
    .orderBy("country")
    .limit(5)
)
