from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("we.session_id") == F.col("s.id"), "right")
    .filter(F.col("s.country").isin(["DE", "UK"]))
    .groupBy(F.col("s.browser"))
    .agg(
        F.max(F.col("we.event_time")).alias("last_event_time"),
        F.count(F.col("we.duration_sec")).alias("duration_sec_count"),
    )
)
