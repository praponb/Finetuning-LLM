from pyspark.sql import functions as F

session_summary = (
    df_web_events
    .groupBy("session_id")
    .agg(F.concat_ws(", ", F.sort_array(F.collect_set("page"))).alias("page_values"))
)

result = (
    df_sessions.alias("s")
    .join(session_summary.alias("t"), F.col("s.id") == F.col("t.session_id"), "left")
    .select(
        F.col("s.id"),
        F.col("s.device"),
        F.col("s.country"),
        F.col("t.page_values"),
    )
)
