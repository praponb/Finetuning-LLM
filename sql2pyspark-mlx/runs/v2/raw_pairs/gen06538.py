from pyspark.sql import functions as F

session_stats = (
    df_web_events
    .groupBy("session_id")
    .agg(
        F.sum("duration_sec").alias("total_duration_sec"),
        (F.max("duration_sec") - F.min("duration_sec")).alias("duration_sec_range"),
    )
)

result = (
    df_sessions.alias("s")
    .join(session_stats.alias("ss"), F.col("s.id") == F.col("ss.session_id"), "inner")
    .filter(F.col("ss.duration_sec_range") >= 74)
    .select(
        F.col("s.browser"),
        F.col("ss.total_duration_sec"),
        F.col("ss.duration_sec_range"),
    )
    .orderBy(F.col("total_duration_sec").desc())
)
