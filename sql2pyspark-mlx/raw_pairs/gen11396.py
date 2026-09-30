from pyspark.sql import functions as F

session_stats = (
    df_web_events
    .filter(F.col("session_id") == 4)
    .groupBy("session_id")
    .agg(F.min("duration_sec").alias("min_duration_sec"))
)

result = (
    df_sessions.alias("s")
    .join(session_stats.alias("t"), F.col("s.id") == F.col("t.session_id"), "inner")
    .filter(F.col("t.min_duration_sec") > 190)
    .select(F.col("s.customer_id"), F.col("t.min_duration_sec"))
    .orderBy(F.col("min_duration_sec").desc())
)
