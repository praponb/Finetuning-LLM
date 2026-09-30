from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("s.id") == F.col("we.session_id"), "left")
    .groupBy(F.to_date(F.col("s.started_at")).alias("started_day"))
    .agg(F.countDistinct(F.col("we.duration_sec")).alias("unique_pages"))
    .orderBy("started_day")
)
