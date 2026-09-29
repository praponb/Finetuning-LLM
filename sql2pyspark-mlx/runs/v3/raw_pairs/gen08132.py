from pyspark.sql import functions as F

session_stats = (
    df_web_events
    .groupBy("session_id")
    .agg(F.count("*").alias("num_web_events"))
)

result = (
    df_sessions.alias("s")
    .join(session_stats.alias("ss"), F.col("s.id") == F.col("ss.session_id"), "left")
    .filter(F.col("ss.num_web_events") >= 4)
    .select(
        F.col("s.id"),
        F.col("s.device"),
        F.col("s.country"),
        F.col("ss.num_web_events"),
    )
)
