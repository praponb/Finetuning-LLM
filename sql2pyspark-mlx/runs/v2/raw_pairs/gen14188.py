from pyspark.sql import functions as F

session_totals = (
    df_web_events
    .groupBy("session_id")
    .agg(
        F.round(F.avg("duration_sec"), 2).alias("avg_duration_sec"),
        F.round(((100 * F.sum(F.when(F.col("event_type") == "purchase", 1).otherwise(0))) / F.count("*")), 1).alias("pct_purchase"),
    )
)

result = (
    df_sessions.alias("s")
    .join(session_totals.alias("st"), F.col("s.id") == F.col("st.session_id"), "inner")
    .select(
        F.col("s.started_at"),
        F.col("st.avg_duration_sec"),
        F.col("st.pct_purchase"),
    )
)
