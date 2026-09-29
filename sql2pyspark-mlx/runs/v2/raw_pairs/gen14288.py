from pyspark.sql import functions as F

session_totals = (
    df_web_events
    .groupBy("session_id")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("event_type"))).alias("event_type_values"),
        F.count("duration_sec").alias("duration_sec_count"),
    )
)

result = (
    df_sessions.alias("s")
    .join(session_totals.alias("st"), F.col("s.id") == F.col("st.session_id"), "inner")
    .select(
        F.col("s.country"),
        F.col("s.browser"),
        F.col("st.event_type_values"),
        F.col("st.duration_sec_count"),
    )
    .orderBy("event_type_values")
)
