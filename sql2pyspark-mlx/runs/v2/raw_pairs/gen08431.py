from pyspark.sql import functions as F

recent_web_events = df_web_events.filter((F.col("session_id") == 3) & (F.col("duration_sec") == 324))

web_events_stats = (
    recent_web_events
    .groupBy("event_type")
    .agg(
        F.sum(F.when(F.col("event_type") == "search", F.col("duration_sec")).otherwise(0)).alias("search_duration_sec"),
        F.max("duration_sec").alias("max_duration_sec"),
    )
)

result = (
    web_events_stats
    .filter(F.col("max_duration_sec") >= 470)
    .orderBy("event_type")
)
