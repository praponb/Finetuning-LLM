from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_web_events.filter(F.col("duration_sec") < 465).alias("we"), F.col("s.id") == F.col("we.session_id"), "left_semi")
    .select(F.col("s.id"), F.col("s.browser"), F.col("s.device"), F.col("s.country"))
    .orderBy("id", F.col("device").desc())
)
