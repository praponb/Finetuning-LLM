from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("we.session_id") == F.col("s.id"), "inner")
    .filter(F.to_date(F.col("we.event_time")) >= "2025-10-01")
    .groupBy(F.col("s.device"), F.col("s.browser"))
    .agg(
        F.sum(F.col("we.duration_sec")).alias("total_duration_sec"),
        F.sum(F.when(F.col("we.event_type") == "add_to_cart", 1).otherwise(0)).alias("add_to_cart_count"),
    )
    .orderBy("device", "browser")
    .limit(5)
)
