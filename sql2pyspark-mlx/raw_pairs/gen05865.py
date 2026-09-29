from pyspark.sql import functions as F

session_totals = (
    df_web_events
    .filter(F.col("event_type").isin(["search", "page_view", "click"]))
    .groupBy("session_id")
    .agg(F.count("duration_sec").alias("duration_sec_count"))
)

result = (
    df_sessions.alias("s")
    .join(session_totals.alias("st"), F.col("s.id") == F.col("st.session_id"), "left")
    .filter(F.col("st.duration_sec_count") >= 5)
    .select(F.col("s.id"), F.col("s.started_at"), F.col("st.duration_sec_count"))
)
