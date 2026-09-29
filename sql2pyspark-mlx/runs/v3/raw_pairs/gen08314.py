from pyspark.sql import functions as F

web_events_by_session = (
    df_web_events
    .filter(F.col("session_id").isin([1, 6, 15, 39]))
    .groupBy("session_id")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("page") == "/search", 1).otherwise(0))) / F.count("*")), 1).alias("pct_search"),
        F.min("event_time").alias("first_event_time"),
    )
)

result = (
    df_sessions.alias("s")
    .join(web_events_by_session.alias("t"), F.col("s.id") == F.col("t.session_id"), "left")
    .select(
        F.col("s.id"),
        F.col("s.country"),
        F.col("t.pct_search"),
        F.col("t.first_event_time"),
    )
    .orderBy(F.col("country").desc())
)
