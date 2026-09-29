from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.filter(F.col("started_at") >= "2024-08-23").alias("s"), F.col("we.session_id") == F.col("s.id"), "left_semi")
    .select(F.col("we.id"), F.col("we.duration_sec"))
    .orderBy("duration_sec")
)
