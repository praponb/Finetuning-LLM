from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("we.session_id") == F.col("s.id"), "left")
    .groupBy(F.hour(F.col("s.started_at")).alias("started_hour"), F.col("s.country"))
    .agg(F.count("*").alias("cnt"))
    .filter(F.col("cnt") > 5)
)
