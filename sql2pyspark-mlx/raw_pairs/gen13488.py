from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("s.id") == F.col("we.session_id"), "inner")
    .filter(F.col("s.country").isin(["SG", "US"]))
    .groupBy(F.col("s.country"))
    .agg(F.count(F.col("we.duration_sec")).alias("duration_sec_count"))
)
