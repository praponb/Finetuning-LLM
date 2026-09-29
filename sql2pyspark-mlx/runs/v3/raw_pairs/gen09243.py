from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_web_events.alias("we"), F.col("we.session_id") == F.col("s.id"), "left")
    .filter(F.to_date(F.col("we.event_time")) < "2024-06-01")
    .groupBy(F.col("s.id"), F.col("s.device"))
    .agg(
        F.count(F.col("we.id")).alias("web_event_count"),
        F.sum(F.when(F.col("we.page") == "/cart", 1).otherwise(0)).alias("cart_count"),
        F.sum(F.when(F.col("we.page") == "/product", F.col("we.duration_sec")).otherwise(0)).alias("product_duration_sec"),
    )
    .orderBy("cart_count", "id", "device")
    .limit(50)
)
