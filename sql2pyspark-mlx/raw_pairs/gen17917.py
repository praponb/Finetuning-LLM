from pyspark.sql import functions as F

web_events_by_session = (
    df_web_events
    .filter(F.col("session_id").isin([10, 30]))
    .groupBy("session_id")
    .agg(
        (F.max("duration_sec") - F.min("duration_sec")).alias("duration_sec_range"),
        F.max("duration_sec").alias("max_duration_sec"),
    )
)

result = (
    df_sessions.alias("s")
    .join(web_events_by_session.alias("webs"), F.col("s.id") == F.col("webs.session_id"), "inner")
    .filter(F.col("webs.duration_sec_range") > 63)
    .select(
        F.col("s.id"),
        F.col("s.browser"),
        F.col("s.device"),
        F.col("webs.duration_sec_range"),
        F.col("webs.max_duration_sec"),
    )
)
