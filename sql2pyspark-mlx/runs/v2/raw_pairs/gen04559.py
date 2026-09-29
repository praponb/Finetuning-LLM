from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_web_events.alias("we"), F.col("s.id") == F.col("we.session_id"), "left")
    .filter(F.col("we.event_type") != "page_view")
    .groupBy(F.col("s.id"))
    .agg(
        F.count(F.col("we.id")).alias("web_event_count"),
        F.round(F.stddev(F.col("we.duration_sec")), 2).alias("stddev_duration_sec"),
    )
    .orderBy(F.col("web_event_count").desc())
)
