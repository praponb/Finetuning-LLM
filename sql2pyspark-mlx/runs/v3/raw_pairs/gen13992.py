from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("we.session_id") == F.col("s.id"), "left")
    .filter(F.col("s.country") == "DE")
    .groupBy(F.col("s.browser"))
    .agg(F.max(F.col("we.event_time")).alias("last_event_time"))
    .orderBy(F.col("browser").desc())
    .limit(1)
)
