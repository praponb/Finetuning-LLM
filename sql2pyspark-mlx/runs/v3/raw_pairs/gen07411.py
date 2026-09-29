from pyspark.sql import functions as F

session_stats = (
    df_web_events
    .groupBy("session_id")
    .agg(
        F.countDistinct("duration_sec").alias("unique_event_types"),
        F.round(((100 * F.sum(F.when(F.col("page") == "/search", 1).otherwise(0))) / F.count("*")), 1).alias("pct_search"),
    )
)

result = (
    df_sessions.alias("s")
    .join(session_stats.alias("ss"), F.col("s.id") == F.col("ss.session_id"), "inner")
    .filter(F.col("ss.pct_search") >= 33)
    .select(F.col("s.device"), F.col("ss.unique_event_types"), F.col("ss.pct_search"))
    .orderBy("device")
)
