from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.filter(F.col("country").isNotNull()).alias("s"), F.col("we.session_id") == F.col("s.id"), "left_anti")
    .select(F.col("we.id"), F.col("we.duration_sec"))
    .orderBy("duration_sec", F.col("id").desc())
    .limit(5)
)
