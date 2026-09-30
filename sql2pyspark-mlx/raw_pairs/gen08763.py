from pyspark.sql import functions as F

session_totals = (
    df_web_events
    .filter(F.col("session_id").isin([7, 31]))
    .groupBy("session_id")
    .agg(
        F.count(F.when(F.col("event_type") == "page_view", 1)).alias("page_view_count"),
        (F.max("duration_sec") - F.min("duration_sec")).alias("duration_sec_range"),
    )
)

result = (
    df_sessions.alias("s")
    .join(session_totals.alias("st"), F.col("s.id") == F.col("st.session_id"), "left")
    .filter(F.col("st.duration_sec_range") >= 56)
    .select(
        F.col("s.id"),
        F.col("s.browser"),
        F.col("s.customer_id"),
        F.col("st.page_view_count"),
        F.col("st.duration_sec_range"),
    )
    .orderBy(F.col("browser").desc())
)
