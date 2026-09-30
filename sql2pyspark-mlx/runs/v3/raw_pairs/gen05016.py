from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_web_events.alias("we"), F.col("s.id") == F.col("we.session_id"), "left")
    .groupBy(F.col("s.id"), F.col("s.device"))
    .agg(
        F.count(F.col("we.id")).alias("web_event_count"),
        F.min(F.col("we.duration_sec")).alias("min_duration_sec"),
        F.round(((100 * F.sum(F.when(F.col("we.page") == "/home", 1).otherwise(0))) / F.count("*")), 1).alias("pct_home"),
    )
    .orderBy("device", "id")
    .limit(20)
)
