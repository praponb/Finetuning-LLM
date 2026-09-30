from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_web_events.alias("we"), F.col("we.session_id") == F.col("s.id"), "inner")
    .filter(F.col("we.duration_sec").between(194, 251))
    .groupBy(F.col("s.browser"))
    .agg(F.count(F.col("we.id")).alias("web_event_count"))
    .orderBy("browser")
)
