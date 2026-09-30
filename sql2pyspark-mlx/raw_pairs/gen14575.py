from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("we.session_id") == F.col("s.id"), "left")
    .groupBy(F.month(F.col("s.started_at")).alias("started_month"))
    .agg(F.count(F.col("we.duration_sec")).alias("duration_sec_count"))
    .filter(F.col("duration_sec_count") >= 5)
)
