from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("we.session_id") == F.col("s.id"), "right")
    .filter(F.col("s.country") == "JP")
    .groupBy(F.col("s.browser"))
    .agg(
        F.min(F.col("we.duration_sec")).alias("min_duration_sec"),
        F.count(F.col("we.duration_sec")).alias("duration_sec_count"),
    )
    .filter(F.col("min_duration_sec") > 130)
)
