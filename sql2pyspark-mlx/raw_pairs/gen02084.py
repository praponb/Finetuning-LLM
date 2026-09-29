from pyspark.sql import functions as F

web_events_by_session = (
    df_web_events
    .groupBy("session_id")
    .agg(
        F.countDistinct("duration_sec").alias("unique_sessions"),
        F.round(F.avg("duration_sec"), 2).alias("avg_duration_sec"),
    )
)

result = (
    df_sessions.alias("s")
    .join(web_events_by_session.alias("webs"), F.col("s.id") == F.col("webs.session_id"), "inner")
    .select(
        F.col("s.country"),
        F.col("s.customer_id"),
        F.col("webs.unique_sessions"),
        F.col("webs.avg_duration_sec"),
    )
    .orderBy(F.col("customer_id").desc())
)
