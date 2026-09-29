from pyspark.sql import functions as F

session_stats = (
    df_web_events
    .groupBy("session_id")
    .agg(
        F.sum(F.when(F.col("page") == "/home", F.col("duration_sec")).otherwise(0)).alias("home_duration_sec"),
        F.sort_array(F.collect_set("page")).alias("page_list"),
    )
)

result = (
    df_sessions.alias("s")
    .join(session_stats.alias("t"), F.col("s.id") == F.col("t.session_id"), "left")
    .select(
        F.col("s.id"),
        F.col("s.browser"),
        F.col("s.country"),
        F.col("t.home_duration_sec"),
        F.col("t.page_list"),
    )
    .orderBy("country")
)
