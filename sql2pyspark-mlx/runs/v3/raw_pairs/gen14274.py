from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.to_date("event_time") < "2025-06-20")
    .groupBy("event_type")
    .agg(
        F.round(F.avg("duration_sec"), 2).alias("avg_duration_sec"),
        F.sum(F.when(F.col("event_type") == "search", F.col("duration_sec")).otherwise(0)).alias("search_duration_sec"),
    )
    .orderBy("avg_duration_sec", "event_type")
    .limit(10)
)
