from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("we.session_id") == F.col("s.id"), "inner")
    .filter(F.col("s.browser") == "Safari")
    .groupBy(F.col("s.country"), F.col("s.browser"))
    .agg(
        F.count(F.col("we.duration_sec")).alias("duration_sec_count"),
        F.countDistinct(F.col("we.duration_sec")).alias("unique_event_types"),
    )
    .orderBy("duration_sec_count", "country", "browser")
    .limit(10)
)
