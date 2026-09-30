from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("we.session_id") == F.col("s.id"), "left")
    .filter(F.col("we.session_id").isin([5, 20]))
    .groupBy(F.col("s.country"))
    .agg(F.max(F.col("we.event_time")).alias("latest_event_time"))
)
