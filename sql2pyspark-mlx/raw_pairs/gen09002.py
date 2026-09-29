from pyspark.sql import functions as F

session_summary = (
    df_web_events
    .groupBy("session_id")
    .agg(
        F.min("duration_sec").alias("min_duration_sec"),
        (F.max("duration_sec") - F.min("duration_sec")).alias("duration_sec_range"),
    )
)

result = (
    df_sessions.alias("s")
    .join(session_summary.alias("ss"), F.col("s.id") == F.col("ss.session_id"), "left")
    .filter(F.col("ss.duration_sec_range") >= 60)
    .select(
        F.col("s.country"),
        F.col("ss.min_duration_sec"),
        F.col("ss.duration_sec_range"),
    )
    .orderBy(F.col("duration_sec_range").desc())
)
