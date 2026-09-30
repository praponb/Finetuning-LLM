from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_web_events.alias("we"), F.col("s.id") == F.col("we.session_id"), "left_anti")
    .filter(F.col("s.started_at") < "2025-07-01")
    .select(F.col("s.id"), F.col("s.customer_id"), F.col("s.device"))
    .orderBy(F.col("device").desc())
)
