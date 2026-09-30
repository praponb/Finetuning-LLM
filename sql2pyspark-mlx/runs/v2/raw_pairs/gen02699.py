from pyspark.sql import functions as F

session_totals = (
    df_web_events
    .filter(F.col("session_id").isin([19, 28]))
    .groupBy("session_id")
    .agg(F.count("duration_sec").alias("duration_sec_count"))
)

result = (
    df_sessions.alias("s")
    .join(session_totals.alias("st"), F.col("s.id") == F.col("st.session_id"), "inner")
    .select(
        F.col("s.id"),
        F.col("s.browser"),
        F.col("s.customer_id"),
        F.col("st.duration_sec_count"),
    )
)
