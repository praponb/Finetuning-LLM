from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_web_events.alias("we"), F.col("we.session_id") == F.col("s.id"), "left")
    .groupBy(F.col("s.id"))
    .agg(F.count(F.col("we.id")).alias("web_event_count"))
    .filter(F.col("web_event_count") >= 5)
    .orderBy(F.col("id").desc(), F.col("web_event_count").desc())
)
