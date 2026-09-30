from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("we.session_id") == F.col("s.id"), "inner")
    .filter(F.col("s.device") == "mobile")
    .groupBy(F.col("s.browser"))
    .agg(
        F.min(F.col("we.duration_sec")).alias("min_duration_sec"),
        F.countDistinct(F.col("we.duration_sec")).alias("unique_pages"),
    )
)
