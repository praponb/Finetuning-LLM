from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("we.session_id") == F.col("s.id"), "inner")
    .groupBy(F.col("s.country"))
    .agg(F.min(F.col("we.duration_sec")).alias("min_duration_sec"))
    .orderBy(F.col("min_duration_sec").desc(), "country")
    .limit(10)
)
