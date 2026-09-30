from pyspark.sql import functions as F

session_totals = (
    df_web_events
    .filter(F.col("duration_sec").between(296, 365))
    .groupBy("session_id")
    .agg(
        (F.max("duration_sec") - F.min("duration_sec")).alias("duration_sec_range"),
        F.concat_ws(", ", F.sort_array(F.collect_set("page"))).alias("page_values"),
    )
)

result = (
    df_sessions.alias("s")
    .join(session_totals.alias("st"), F.col("s.id") == F.col("st.session_id"), "inner")
    .filter(F.col("st.duration_sec_range") > 72)
    .select(
        F.col("s.customer_id"),
        F.col("s.started_at"),
        F.col("st.duration_sec_range"),
        F.col("st.page_values"),
    )
)
