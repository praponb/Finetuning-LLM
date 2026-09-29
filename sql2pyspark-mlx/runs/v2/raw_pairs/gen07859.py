from pyspark.sql import functions as F

session_summary = (
    df_web_events
    .groupBy("session_id")
    .agg(
        F.sum(F.when(F.col("page") == "/home", 1).otherwise(0)).alias("home_count"),
        F.avg("duration_sec").alias("avg_duration_sec"),
    )
)

result = (
    df_sessions.alias("s")
    .join(session_summary.alias("ss"), F.col("s.id") == F.col("ss.session_id"), "inner")
    .select(
        F.col("s.id"),
        F.col("s.country"),
        F.col("s.device"),
        F.col("ss.home_count"),
        F.col("ss.avg_duration_sec"),
    )
    .orderBy(F.col("country").desc())
)
