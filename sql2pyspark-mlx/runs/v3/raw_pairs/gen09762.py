from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("we.session_id") == F.col("s.id"), "inner")
    .filter(F.col("we.session_id").isin([2, 24, 37]))
    .groupBy(F.month(F.col("s.started_at")).alias("started_month"))
    .agg(F.max(F.col("we.event_time")).alias("latest_event_time"))
)
