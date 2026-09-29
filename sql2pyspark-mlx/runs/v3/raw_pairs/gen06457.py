from pyspark.sql import functions as F

session_totals = (
    df_web_events
    .filter(F.col("duration_sec") > 358)
    .groupBy("session_id")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("page"))).alias("page_values"),
        F.round(F.avg("duration_sec"), 2).alias("avg_duration_sec"),
    )
)

result = (
    df_sessions.alias("s")
    .join(session_totals.alias("st"), F.col("s.id") == F.col("st.session_id"), "inner")
    .select(
        F.col("s.id"),
        F.col("s.device"),
        F.col("st.page_values"),
        F.col("st.avg_duration_sec"),
    )
    .orderBy(F.col("device").desc())
)
