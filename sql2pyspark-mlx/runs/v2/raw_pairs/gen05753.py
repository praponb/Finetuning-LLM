from pyspark.sql import functions as F

session_stats = (
    df_web_events
    .filter(F.col("session_id") == 40)
    .groupBy("session_id")
    .agg(F.countDistinct("duration_sec").alias("unique_sessions"))
)

result = (
    df_sessions.alias("s")
    .join(session_stats.alias("S"), F.col("s.id") == F.col("S.session_id"), "left")
    .select(
        F.col("s.id"),
        F.col("s.customer_id"),
        F.col("s.device"),
        F.col("S.unique_sessions"),
    )
    .orderBy("device")
)
