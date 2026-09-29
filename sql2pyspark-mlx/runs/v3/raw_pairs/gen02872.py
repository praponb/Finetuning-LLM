from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_web_events.alias("we"), F.col("s.id") == F.col("we.session_id"), "left_anti")
    .filter(F.col("s.started_at") < "2025-10-27")
    .select(F.col("s.id"), F.col("s.country"), F.col("s.device"), F.col("s.started_at"))
    .orderBy(F.col("started_at").desc(), "device")
)
