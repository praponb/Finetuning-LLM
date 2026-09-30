from pyspark.sql import functions as F

web_events_by_session = (
    df_web_events
    .groupBy("session_id")
    .agg(
        F.max("duration_sec").alias("max_duration_sec"),
        F.round(F.avg("duration_sec"), 2).alias("avg_duration_sec"),
    )
)

result = (
    df_sessions.alias("s")
    .join(web_events_by_session.alias("webs"), F.col("s.id") == F.col("webs.session_id"), "left")
    .select(
        F.col("s.id"),
        F.col("s.device"),
        F.col("s.country"),
        F.col("webs.max_duration_sec"),
        F.col("webs.avg_duration_sec"),
    )
    .orderBy("max_duration_sec")
)
