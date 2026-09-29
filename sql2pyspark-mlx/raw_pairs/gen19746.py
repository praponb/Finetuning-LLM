from pyspark.sql import functions as F

session_totals = (
    df_web_events
    .groupBy("session_id")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("page"))).alias("page_values"),
        (F.max("duration_sec") - F.min("duration_sec")).alias("duration_sec_range"),
    )
)

result = (
    df_sessions.alias("s")
    .join(session_totals.alias("st"), F.col("s.id") == F.col("st.session_id"), "left")
    .filter(F.col("st.duration_sec_range") >= 64)
    .select(
        F.col("s.id"),
        F.col("s.started_at"),
        F.col("s.country"),
        F.col("st.page_values"),
        F.col("st.duration_sec_range"),
    )
)
