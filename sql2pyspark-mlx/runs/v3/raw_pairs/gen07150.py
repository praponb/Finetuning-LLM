from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("s.id") == F.col("we.session_id"), "inner")
    .groupBy(F.col("s.browser"))
    .agg(
        F.max(F.col("we.event_time")).alias("latest_event_time"),
        F.count(F.when(F.col("we.page") == "/checkout", 1)).alias("checkout_count"),
    )
    .orderBy("latest_event_time")
)
