from pyspark.sql import functions as F

session_stats = (
    df_web_events
    .groupBy("session_id")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("page"))).alias("page_values"),
        F.round(F.stddev("duration_sec"), 2).alias("stddev_duration_sec"),
    )
)

result = (
    df_sessions.alias("s")
    .join(session_stats.alias("ss"), F.col("s.id") == F.col("ss.session_id"), "left")
    .filter(F.col("ss.stddev_duration_sec") >= 42)
    .select(
        F.col("s.browser"),
        F.col("s.country"),
        F.col("ss.page_values"),
        F.col("ss.stddev_duration_sec"),
    )
)
