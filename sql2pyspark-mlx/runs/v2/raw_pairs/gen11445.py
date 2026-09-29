from pyspark.sql import functions as F

session_totals = (
    df_web_events
    .groupBy("session_id")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("page") == "/search", 1).otherwise(0))) / F.count("*")), 1).alias("pct_search"),
        F.min("duration_sec").alias("min_duration_sec"),
    )
)

result = (
    df_sessions.alias("s")
    .join(session_totals.alias("st"), F.col("s.id") == F.col("st.session_id"), "inner")
    .filter(F.col("st.pct_search") >= 37)
    .select(
        F.col("s.id"),
        F.col("s.started_at"),
        F.col("st.pct_search"),
        F.col("st.min_duration_sec"),
    )
    .orderBy(F.col("pct_search").desc())
)
