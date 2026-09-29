from pyspark.sql import functions as F

session_summary = (
    df_web_events
    .filter(F.col("duration_sec").between(146, 319))
    .groupBy("session_id")
    .agg(
        F.sum(F.when(F.col("event_type") == "page_view", F.col("duration_sec")).otherwise(0)).alias("page_view_duration_sec"),
    )
)

result = (
    df_sessions.alias("s")
    .join(session_summary.alias("ss"), F.col("s.id") == F.col("ss.session_id"), "left")
    .select(
        F.col("s.customer_id"),
        F.col("s.device"),
        F.col("ss.page_view_duration_sec"),
    )
)
