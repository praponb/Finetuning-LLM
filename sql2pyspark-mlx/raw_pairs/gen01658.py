from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("s.id") == F.col("we.session_id"), "inner")
    .groupBy(F.col("s.browser"))
    .agg(
        F.sum(F.when(F.col("we.page") == "/cart", 1).otherwise(0)).alias("cart_count"),
        F.max(F.col("we.event_time")).alias("latest_event_time"),
    )
    .orderBy("latest_event_time")
)
