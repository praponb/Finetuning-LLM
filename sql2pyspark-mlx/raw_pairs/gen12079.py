from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_web_events.filter(F.to_date("event_time") < "2025-09-01").alias("we"), F.col("s.id") == F.col("we.session_id"), "left_anti")
    .filter(F.hour(F.col("s.started_at")).between(7, 12))
    .select(F.col("s.id"), F.col("s.browser"))
    .orderBy("id", "browser")
    .limit(5)
)
