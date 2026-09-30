from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_web_events.alias("we"), F.col("s.id") == F.col("we.session_id"), "inner")
    .groupBy(F.col("s.id"), F.col("s.device"))
    .agg(
        F.count(F.col("we.id")).alias("web_event_count"),
        F.count(F.col("we.duration_sec")).alias("duration_sec_count"),
    )
    .orderBy(F.col("id").desc())
)
