from pyspark.sql import functions as F

session_totals = (
    df_web_events
    .groupBy("session_id")
    .agg(
        F.count("duration_sec").alias("duration_sec_count"),
        (F.max("duration_sec") - F.min("duration_sec")).alias("duration_sec_range"),
    )
)

result = (
    df_sessions.alias("sessions")
    .join(session_totals.alias("t"), F.col("sessions.id") == F.col("t.session_id"), "inner")
    .select(
        F.col("sessions.country"),
        F.col("t.duration_sec_count"),
        F.col("t.duration_sec_range"),
    )
)
