from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_web_events.alias("we"), F.col("s.id") == F.col("we.session_id"), "left")
    .filter(F.col("we.duration_sec").between(369, 416))
    .groupBy(F.col("s.id"), F.col("s.device"))
    .agg(F.count(F.col("we.id")).alias("web_event_count"))
)
