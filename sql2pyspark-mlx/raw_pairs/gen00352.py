from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("we.session_id") == F.col("s.id"), "right")
    .groupBy(F.col("s.device"), F.col("s.browser"))
    .agg(
        F.avg(F.col("we.duration_sec")).alias("avg_duration_sec"),
        F.max(F.col("we.duration_sec")).alias("max_duration_sec"),
    )
    .filter(F.col("max_duration_sec") >= 320)
    .orderBy("avg_duration_sec")
)
