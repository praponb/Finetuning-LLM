from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("we.session_id") == F.col("s.id"), "inner")
    .filter(F.col("we.session_id").isin([17, 21, 27, 33]))
    .groupBy(F.col("s.browser"))
    .agg(F.count("*").alias("total_count"))
    .orderBy("total_count")
)
