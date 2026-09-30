from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.filter(F.col("country") == "UK").alias("s"), F.col("we.session_id") == F.col("s.id"), "left_semi")
    .select(F.col("we.id"), F.col("we.session_id"), F.col("we.event_time"))
    .orderBy(F.col("event_time").desc(), F.col("id").desc())
)
