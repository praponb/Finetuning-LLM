from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_web_events.alias("we"), F.col("we.session_id") == F.col("s.id"), "inner")
    .groupBy(F.col("s.id"), F.col("s.browser"))
    .agg(
        F.count(F.col("we.id")).alias("web_event_count"),
        F.sum(F.col("we.duration_sec")).alias("total_duration_sec"),
    )
    .filter(F.col("web_event_count") >= 2)
    .orderBy(F.col("web_event_count").desc())
)
