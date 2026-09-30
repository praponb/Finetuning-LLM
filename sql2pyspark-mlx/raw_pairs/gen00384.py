from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("we.session_id") == F.col("s.id"), "left")
    .groupBy(F.col("s.browser"))
    .agg(F.round(F.avg(F.col("we.duration_sec")), 2).alias("avg_duration_sec"))
    .orderBy(F.col("avg_duration_sec").desc(), "browser")
    .limit(10)
)
