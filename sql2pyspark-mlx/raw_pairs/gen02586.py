from pyspark.sql import functions as F

session_totals = (
    df_web_events
    .filter(F.col("session_id") == 24)
    .groupBy("session_id")
    .agg(
        F.max("duration_sec").alias("max_duration_sec"),
        (F.max("duration_sec") - F.min("duration_sec")).alias("duration_sec_range"),
    )
)

result = (
    df_sessions.alias("s")
    .join(session_totals.alias("st"), F.col("s.id") == F.col("st.session_id"), "inner")
    .select(
        F.col("s.id"),
        F.col("s.started_at"),
        F.col("st.max_duration_sec"),
        F.col("st.duration_sec_range"),
    )
)
