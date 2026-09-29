from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.filter(F.col("started_at") >= "2024-07-25").alias("s"), F.col("we.session_id") == F.col("s.id"), "left_anti")
    .filter(F.col("we.duration_sec") >= 403)
    .select(F.col("we.id"), F.col("we.duration_sec"))
    .orderBy(F.col("duration_sec").desc())
)
