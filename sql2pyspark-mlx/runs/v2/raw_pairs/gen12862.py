from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_web_events.alias("we"), F.col("s.id") == F.col("we.session_id"), "left")
    .filter(F.col("we.event_time") < "2025-04-01")
    .groupBy(F.col("s.id"))
    .agg(
        F.count(F.col("we.id")).alias("web_event_count"),
        F.count(F.col("we.duration_sec")).alias("duration_sec_count"),
    )
    .orderBy(F.col("web_event_count").desc())
)
