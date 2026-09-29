from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_web_events.filter(F.col("event_type") != "click").alias("we"), F.col("s.id") == F.col("we.session_id"), "left_anti")
    .filter(F.col("s.country") == "TH")
    .select(F.col("s.id"), F.col("s.country"), F.col("s.device"))
    .orderBy(F.col("id").desc())
    .limit(10)
)
