from pyspark.sql import functions as F

session_totals = (
    df_web_events
    .groupBy("session_id")
    .agg(F.max("duration_sec").alias("max_duration_sec"))
)

result = (
    df_sessions.alias("s")
    .join(session_totals.alias("st"), F.col("s.id") == F.col("st.session_id"), "left")
    .select(F.col("s.browser"), F.col("s.customer_id"), F.col("st.max_duration_sec"))
)
