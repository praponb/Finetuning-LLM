from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy(F.month("event_time").alias("event_month"))
    .agg(
        F.max("event_time").alias("last_event_time"),
        F.sum(F.when(F.col("event_type") == "page_view", F.col("duration_sec")).otherwise(0)).alias("page_view_duration_sec"),
    )
    .filter(F.col("page_view_duration_sec") >= 40)
    .orderBy("page_view_duration_sec", "event_month")
    .limit(5)
)
