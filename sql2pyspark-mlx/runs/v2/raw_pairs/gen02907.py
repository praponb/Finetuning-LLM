from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("we.session_id") == F.col("s.id"), "inner")
    .filter(F.col("we.event_time") >= "2025-04-22")
    .groupBy(F.date_format(F.col("s.started_at"), "yyyy-MM").alias("started_month"))
    .agg(
        F.count(F.when(F.col("we.event_type") == "add_to_cart", 1)).alias("add_to_cart_count"),
        (F.max(F.col("we.duration_sec")) - F.min(F.col("we.duration_sec"))).alias("duration_sec_range"),
    )
)
