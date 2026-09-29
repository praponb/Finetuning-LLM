from pyspark.sql import functions as F

session_stats = (
    df_web_events
    .groupBy("session_id")
    .agg(
        F.max("duration_sec").alias("max_duration_sec"),
        F.concat_ws(", ", F.sort_array(F.collect_set("page"))).alias("page_values"),
    )
)

result = (
    df_sessions.alias("s")
    .join(session_stats.alias("ss"), F.col("s.id") == F.col("ss.session_id"), "inner")
    .select(
        F.col("s.id"),
        F.col("s.customer_id"),
        F.col("s.browser"),
        F.col("ss.max_duration_sec"),
        F.col("ss.page_values"),
    )
    .orderBy("id", "page_values")
    .limit(1)
)
