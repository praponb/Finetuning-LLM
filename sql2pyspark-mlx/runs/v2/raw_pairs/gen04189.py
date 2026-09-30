from pyspark.sql import functions as F

session_stats = (
    df_web_events
    .filter(F.col("page").isNull())
    .groupBy("session_id")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("event_type") == "click", 1).otherwise(0))) / F.count("*")), 1).alias("pct_click"),
    )
)

result = (
    df_sessions.alias("s")
    .join(session_stats.alias("ss"), F.col("s.id") == F.col("ss.session_id"), "inner")
    .select(F.col("s.browser"), F.col("s.country"), F.col("ss.pct_click"))
    .orderBy("country")
)
