from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("s.id") == F.col("we.session_id"), "inner")
    .filter(F.col("s.device") == "desktop")
    .groupBy(F.col("s.country"), F.col("s.browser"))
    .agg(
        F.min(F.col("we.event_time")).alias("min_event_time"),
        F.max(F.col("we.event_time")).alias("latest_event_time"),
    )
    .orderBy("country")
)
