from pyspark.sql import functions as F

session_stats = (
    df_web_events
    .filter(F.col("event_time") < "2025-04-01")
    .groupBy("session_id")
    .agg(
        F.sum(F.when(F.col("page") == "/product", F.col("duration_sec")).otherwise(0)).alias("product_duration_sec"),
    )
)

result = (
    df_sessions.alias("s")
    .join(session_stats.alias("ss"), F.col("s.id") == F.col("ss.session_id"), "inner")
    .filter(F.col("ss.product_duration_sec") > 58)
    .select(F.col("s.id"), F.col("s.started_at"), F.col("ss.product_duration_sec"))
    .orderBy(F.col("product_duration_sec").desc())
)
