from pyspark.sql import functions as F

valid_web_events = (
    df_web_events
    .filter((F.col("event_time") < "2025-07-01") | F.col("session_id").isin([10, 30]))
)

web_events_stats = (
    valid_web_events
    .groupBy(F.hour("event_time").alias("event_hour"))
    .agg(
        F.sum(F.when(F.col("page") == "/search", 1).otherwise(0)).alias("search_count"),
    )
)

result = web_events_stats.orderBy(F.col("search_count").desc(), F.col("event_hour").desc())
