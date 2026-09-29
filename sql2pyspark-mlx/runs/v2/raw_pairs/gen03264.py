from pyspark.sql import functions as F

session_summary = (
    df_web_events
    .groupBy("session_id")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("event_type") == "page_view", 1).otherwise(0))) / F.count("*")), 1).alias("pct_page_view"),
    )
)

result = (
    df_sessions.alias("sessions")
    .join(session_summary.alias("session_summary"), F.col("sessions.id") == F.col("session_summary.session_id"), "left")
    .filter(F.col("session_summary.pct_page_view") >= 77)
    .select(
        F.col("sessions.device"),
        F.col("sessions.country"),
        F.col("session_summary.pct_page_view"),
    )
)
