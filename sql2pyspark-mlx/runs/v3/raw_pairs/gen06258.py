from pyspark.sql import functions as F

session_totals = (
    df_web_events
    .filter(F.col("event_time") < "2024-02-11")
    .groupBy("session_id")
    .agg(
        F.sum(F.when(F.col("event_type") == "click", F.col("duration_sec")).otherwise(0)).alias("click_duration_sec"),
        F.avg("duration_sec").alias("avg_duration_sec"),
    )
)

result = (
    df_sessions.alias("s")
    .join(session_totals.alias("st"), F.col("s.id") == F.col("st.session_id"), "left")
    .select(
        F.col("s.id"),
        F.col("s.started_at"),
        F.col("s.customer_id"),
        F.col("st.click_duration_sec"),
        F.col("st.avg_duration_sec"),
    )
)
