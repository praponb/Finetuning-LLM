from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_web_events.alias("we"), F.col("we.session_id") == F.col("s.id"), "inner")
    .filter(F.col("s.started_at") >= "2023-09-01")
    .groupBy(F.col("s.id"), F.col("s.browser"))
    .agg(
        F.count(F.col("we.id")).alias("web_event_count"),
        F.max(F.col("we.event_time")).alias("latest_event_time"),
        F.min(F.col("we.event_time")).alias("first_event_time"),
    )
)
