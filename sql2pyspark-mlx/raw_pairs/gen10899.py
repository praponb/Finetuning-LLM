from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_web_events.filter(F.col("duration_sec") < 474).alias("we"), F.col("s.id") == F.col("we.session_id"), "left_semi")
    .filter(F.hour(F.col("s.started_at")).between(7, 10))
    .select(F.col("s.id"), F.col("s.country"))
    .orderBy(F.col("country").desc())
)
