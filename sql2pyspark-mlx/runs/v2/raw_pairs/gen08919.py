from pyspark.sql import functions as F

session_stats = (
    df_web_events
    .groupBy("session_id")
    .agg(F.countDistinct("duration_sec").alias("unique_pages"))
)

result = (
    df_sessions.alias("s")
    .join(session_stats.alias("t"), F.col("s.id") == F.col("t.session_id"), "inner")
    .select(F.col("s.customer_id"), F.col("s.country"), F.col("t.unique_pages"))
    .orderBy(F.col("customer_id").desc())
)
