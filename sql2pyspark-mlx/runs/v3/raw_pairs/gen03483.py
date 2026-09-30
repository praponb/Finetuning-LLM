from pyspark.sql import functions as F

session_totals = (
    df_web_events
    .groupBy("session_id")
    .agg(F.max("event_time").alias("last_event_time"))
)

result = (
    df_sessions.alias("s")
    .join(session_totals.alias("st"), F.col("s.id") == F.col("st.session_id"), "left")
    .select(F.col("s.started_at"), F.col("st.last_event_time"))
    .orderBy("started_at", "last_event_time")
)
