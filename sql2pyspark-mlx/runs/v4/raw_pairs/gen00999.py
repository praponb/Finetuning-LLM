from pyspark.sql import functions as F

web_events_by_session = (
    df_web_events
    .filter(F.col("session_id").isin([1, 3, 5, 20]))
    .groupBy("session_id")
    .agg(F.round(F.stddev("duration_sec"), 2).alias("stddev_duration_sec"))
)

result = (
    df_sessions.alias("s")
    .join(web_events_by_session.alias("webs"), F.col("s.id") == F.col("webs.session_id"), "inner")
    .filter(F.col("webs.stddev_duration_sec") > 72)
    .select(
        F.col("s.id"),
        F.col("s.device"),
        F.col("s.started_at"),
        F.col("webs.stddev_duration_sec"),
    )
    .orderBy(F.col("started_at").desc(), "id")
    .limit(50)
)
