from pyspark.sql import functions as F

session_totals = (
    df_web_events
    .filter(~F.col("event_type").isin(["search", "page_view", "purchase"]))
    .groupBy("session_id")
    .agg(
        F.countDistinct("duration_sec").alias("unique_pages"),
        F.count("duration_sec").alias("duration_sec_count"),
    )
)

result = (
    df_sessions.alias("s")
    .join(session_totals.alias("st"), F.col("s.id") == F.col("st.session_id"), "inner")
    .select(F.col("s.device"), F.col("st.unique_pages"), F.col("st.duration_sec_count"))
    .orderBy(F.col("unique_pages").desc())
)
