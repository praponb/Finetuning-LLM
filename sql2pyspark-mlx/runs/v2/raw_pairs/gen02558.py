from pyspark.sql import functions as F

session_totals = (
    df_web_events
    .groupBy("session_id")
    .agg(F.count("*").alias("total_count"))
)

result = (
    df_sessions.alias("s")
    .join(session_totals.alias("t"), F.col("s.id") == F.col("t.session_id"), "inner")
    .filter(F.col("t.total_count") >= 9)
    .select(F.col("s.id"), F.col("s.country"), F.col("t.total_count"))
    .orderBy(F.col("id").desc(), F.col("total_count").desc())
)
