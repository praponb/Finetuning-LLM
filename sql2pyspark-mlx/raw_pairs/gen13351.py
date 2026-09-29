from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_web_events.alias("we"), F.col("s.id") == F.col("we.session_id"), "left")
    .groupBy(F.col("s.device"))
    .agg(
        F.count(F.col("we.id")).alias("web_event_count"),
        F.sum(F.when(F.col("we.event_type") == "search", 1).otherwise(0)).alias("search_count"),
        F.avg(F.col("we.duration_sec")).alias("avg_duration_sec"),
    )
    .orderBy(F.col("avg_duration_sec").desc())
)
