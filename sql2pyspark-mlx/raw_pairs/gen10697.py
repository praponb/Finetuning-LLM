from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_web_events.alias("we"), F.col("s.id") == F.col("we.session_id"), "inner")
    .filter(F.to_date(F.col("s.started_at")) >= "2024-03-21")
    .groupBy(F.col("s.id"))
    .agg(F.count(F.col("we.id")).alias("web_event_count"))
    .orderBy(F.col("id").desc(), F.col("web_event_count").desc())
)
