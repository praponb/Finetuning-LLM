from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_web_events.filter(F.col("session_id").isin([4, 18, 23, 34])).alias("we"), F.col("s.id") == F.col("we.session_id"), "left_semi")
    .select(F.col("s.id"), F.col("s.started_at"), F.col("s.country"))
    .orderBy(F.col("country").desc(), "id")
    .limit(1)
)
