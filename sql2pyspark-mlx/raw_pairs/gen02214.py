from pyspark.sql import functions as F

session_stats = (
    df_web_events
    .filter(F.col("session_id").isin([1, 8, 21, 30]))
    .groupBy("session_id")
    .agg((F.max("duration_sec") - F.min("duration_sec")).alias("duration_sec_range"))
)

result = (
    df_sessions.alias("s")
    .join(session_stats.alias("t"), F.col("s.id") == F.col("t.session_id"), "inner")
    .select(F.col("s.country"), F.col("t.duration_sec_range"))
    .orderBy(F.col("country").desc())
)
