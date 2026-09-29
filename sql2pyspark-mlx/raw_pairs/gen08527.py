from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.filter(F.to_date("started_at") < "2023-03-01").alias("s"), F.col("we.session_id") == F.col("s.id"), "left_semi")
    .select(F.col("we.id"), F.col("we.event_type"))
    .orderBy(F.col("event_type").desc(), F.col("id").desc())
)
