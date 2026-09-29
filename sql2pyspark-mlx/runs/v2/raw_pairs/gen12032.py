from pyspark.sql import functions as F

web_events_by_session = (
    df_web_events
    .filter(F.col("session_id").isin([12, 16, 18, 31]))
    .groupBy("session_id")
    .agg(
        F.min("duration_sec").alias("min_duration_sec"),
        F.max("duration_sec").alias("max_duration_sec"),
    )
)

result = (
    df_sessions.alias("s")
    .join(web_events_by_session.alias("webs"), F.col("s.id") == F.col("webs.session_id"), "inner")
    .select(
        F.col("s.id"),
        F.col("s.country"),
        F.col("s.browser"),
        F.col("webs.min_duration_sec"),
        F.col("webs.max_duration_sec"),
    )
)
