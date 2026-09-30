from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_web_events.filter(F.col("session_id").isin([13, 27, 34, 37])).alias("we"), F.col("s.id") == F.col("we.session_id"), "left_anti")
    .filter(F.hour(F.col("s.started_at")).between(7, 11))
    .select(F.col("s.id"), F.col("s.started_at"))
    .orderBy("started_at")
)
