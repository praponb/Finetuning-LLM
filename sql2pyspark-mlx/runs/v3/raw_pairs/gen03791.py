from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_web_events.alias("we"), F.col("we.session_id") == F.col("s.id"), "inner")
    .groupBy(F.col("s.country"))
    .agg(F.count(F.col("we.id")).alias("web_event_count"))
    .orderBy("country")
)
