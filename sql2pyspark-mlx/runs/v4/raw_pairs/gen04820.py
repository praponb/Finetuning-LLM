from pyspark.sql import functions as F

session_stats = (
    df_web_events
    .filter(F.col("duration_sec") > 288)
    .groupBy("session_id")
    .agg(F.concat_ws(", ", F.sort_array(F.collect_set("page"))).alias("page_values"))
)

result = (
    df_sessions.alias("s")
    .join(session_stats.alias("ss"), F.col("s.id") == F.col("ss.session_id"), "left")
    .select(
        F.col("s.id"),
        F.col("s.browser"),
        F.col("s.device"),
        F.col("ss.page_values"),
    )
    .orderBy("page_values")
)
