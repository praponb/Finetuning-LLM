from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_web_events.alias("we"), F.col("we.session_id") == F.col("s.id"), "left")
    .groupBy(F.col("s.country"))
    .agg(
        F.count(F.col("we.id")).alias("web_event_count"),
        F.sum(F.when(F.col("we.page") == "/cart", F.col("we.duration_sec")).otherwise(0)).alias("cart_duration_sec"),
        F.sum(F.when(F.col("we.event_type") == "add_to_cart", 1).otherwise(0)).alias("add_to_cart_count"),
    )
    .filter(F.col("cart_duration_sec") >= 66)
)
