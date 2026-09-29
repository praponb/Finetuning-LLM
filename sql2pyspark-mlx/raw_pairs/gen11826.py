from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_web_events.filter(F.col("session_id").isin([8, 38])).alias("we"), F.col("s.id") == F.col("we.session_id"), "left_anti")
    .filter(F.col("s.started_at") < "2025-07-01")
    .select(F.col("s.id"), F.col("s.customer_id"))
    .orderBy(F.col("id").desc())
)
