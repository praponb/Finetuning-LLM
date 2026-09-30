from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("we.session_id") == F.col("s.id"), "inner")
    .groupBy(F.quarter(F.col("s.started_at")).alias("started_quarter"))
    .agg(F.avg(F.col("we.duration_sec")).alias("avg_duration_sec"))
    .filter(F.col("avg_duration_sec") >= 200)
    .orderBy(F.col("started_quarter").desc())
    .limit(3)
)
