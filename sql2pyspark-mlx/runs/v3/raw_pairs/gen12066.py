from pyspark.sql import functions as F

session_totals = (
    df_web_events
    .groupBy("session_id")
    .agg(
        F.avg("duration_sec").alias("avg_duration_sec"),
        F.max("event_time").alias("latest_event_time"),
    )
)

result = (
    df_sessions.alias("s")
    .join(session_totals.alias("st"), F.col("s.id") == F.col("st.session_id"), "inner")
    .filter(F.col("st.avg_duration_sec") >= 420)
    .select(
        F.col("s.country"),
        F.col("st.avg_duration_sec"),
        F.col("st.latest_event_time"),
    )
    .orderBy("latest_event_time")
)
