from pyspark.sql import functions as F

session_stats = (
    df_web_events
    .filter(F.col("event_time") < "2025-03-01")
    .groupBy("session_id")
    .agg(F.concat_ws(", ", F.sort_array(F.collect_set("page"))).alias("page_values"))
)

result = (
    df_sessions.alias("s")
    .join(session_stats.alias("t"), F.col("s.id") == F.col("t.session_id"), "inner")
    .select(F.col("s.browser"), F.col("t.page_values"))
    .orderBy("page_values", F.col("browser").desc())
)
