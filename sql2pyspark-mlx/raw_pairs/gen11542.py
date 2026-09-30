from pyspark.sql import functions as F

session_summary = (
    df_web_events
    .groupBy("session_id")
    .agg(
        F.sum("duration_sec").alias("total_duration_sec"),
        F.avg("duration_sec").alias("avg_duration_sec"),
    )
)

result = (
    df_sessions.alias("s")
    .join(session_summary.alias("ss"), F.col("s.id") == F.col("ss.session_id"), "left")
    .select(
        F.col("s.country"),
        F.col("ss.total_duration_sec"),
        F.col("ss.avg_duration_sec"),
    )
    .orderBy(F.col("total_duration_sec").desc(), "avg_duration_sec")
)
