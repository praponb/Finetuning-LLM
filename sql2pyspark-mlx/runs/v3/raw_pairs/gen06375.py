from pyspark.sql import functions as F

session_totals = (
    df_web_events
    .groupBy("session_id")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("page"))).alias("page_values"),
        F.sum("duration_sec").alias("total_duration_sec"),
    )
)

result = (
    df_sessions.alias("s")
    .join(session_totals.alias("st"), F.col("s.id") == F.col("st.session_id"), "inner")
    .filter(F.col("st.total_duration_sec") > 1900)
    .select(
        F.col("s.started_at"),
        F.col("st.page_values"),
        F.col("st.total_duration_sec"),
    )
)
