from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("we.session_id") == F.col("s.id"), "inner")
    .filter(F.col("we.page") != "/home")
    .groupBy(F.col("s.device"))
    .agg(F.max(F.col("we.event_time")).alias("latest_event_time"))
    .orderBy("latest_event_time")
)
