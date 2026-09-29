from pyspark.sql import functions as F

session_totals = (
    df_web_events
    .groupBy("session_id")
    .agg(
        F.avg("duration_sec").alias("avg_duration_sec"),
        F.sum("duration_sec").alias("total_duration_sec"),
    )
)

result = (
    df_sessions.alias("s")
    .join(session_totals.alias("st"), F.col("s.id") == F.col("st.session_id"), "inner")
    .filter(F.col("st.total_duration_sec") >= 2300)
    .select(
        F.col("s.id"),
        F.col("s.customer_id"),
        F.col("st.avg_duration_sec"),
        F.col("st.total_duration_sec"),
    )
)
