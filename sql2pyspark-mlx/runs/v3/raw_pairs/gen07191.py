from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_web_events.filter(F.col("event_time") < "2023-12-01").alias("we"), F.col("s.id") == F.col("we.session_id"), "left_anti")
    .filter(F.col("s.country") == "UK")
    .select(F.col("s.id"), F.col("s.device"), F.col("s.browser"))
)
