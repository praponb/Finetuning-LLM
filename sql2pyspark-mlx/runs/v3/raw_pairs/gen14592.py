from pyspark.sql import functions as F

session_stats = (
    df_web_events
    .filter(F.col("page") == "/cart")
    .groupBy("session_id")
    .agg(F.min("event_time").alias("min_event_time"))
)

result = (
    df_sessions.alias("s")
    .join(session_stats.alias("S"), F.col("s.id") == F.col("S.session_id"), "inner")
    .select(
        F.col("s.id"),
        F.col("s.country"),
        F.col("s.browser"),
        F.col("S.min_event_time"),
    )
    .orderBy(F.col("id").desc(), F.col("min_event_time").desc())
    .limit(5)
)
