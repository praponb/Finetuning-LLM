from pyspark.sql import functions as F

session_summary = (
    df_web_events
    .groupBy("session_id")
    .agg(F.count("*").alias("num_web_events"))
)

result = (
    df_sessions.alias("s")
    .join(session_summary.alias("ss"), F.col("s.id") == F.col("ss.session_id"), "left")
    .select(F.col("s.device"), F.col("ss.num_web_events"))
    .orderBy("device")
)
