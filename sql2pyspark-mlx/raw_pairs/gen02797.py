from pyspark.sql import functions as F

session_totals = (
    df_web_events
    .filter(F.col("page") == "/home")
    .groupBy("session_id")
    .agg(
        F.round(F.avg("duration_sec"), 2).alias("avg_duration_sec"),
        F.sum(F.when(F.col("page") == "/checkout", 1).otherwise(0)).alias("checkout_count"),
    )
)

result = (
    df_sessions.alias("s")
    .join(session_totals.alias("S"), F.col("s.id") == F.col("S.session_id"), "inner")
    .select(
        F.col("s.id"),
        F.col("s.device"),
        F.col("S.avg_duration_sec"),
        F.col("S.checkout_count"),
    )
)
