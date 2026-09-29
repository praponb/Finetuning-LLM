from pyspark.sql import functions as F

web_events_by_session = (
    df_web_events
    .groupBy("session_id")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("page") == "/home", 1).otherwise(0))) / F.count("*")), 1).alias("pct_home"),
        F.max("event_time").alias("latest_event_time"),
    )
)

result = (
    df_sessions.alias("s")
    .join(web_events_by_session.alias("W"), F.col("s.id") == F.col("W.session_id"), "inner")
    .filter(F.col("W.pct_home") > 49)
    .select(
        F.col("s.id"),
        F.col("s.browser"),
        F.col("W.pct_home"),
        F.col("W.latest_event_time"),
    )
    .orderBy("latest_event_time", "id")
    .limit(50)
)
