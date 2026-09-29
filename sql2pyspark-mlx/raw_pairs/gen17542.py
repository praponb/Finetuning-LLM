from pyspark.sql import functions as F

valid_web_events = (
    df_web_events
    .filter((F.col("event_time") < "2025-10-01") & (F.col("duration_sec") == 138))
    .withColumn("event_time_day", F.dayofmonth("event_time"))
)

web_events_stats = (
    valid_web_events
    .groupBy("page", F.hour("event_time").alias("event_hour"))
    .agg(
        F.round(F.avg("event_time_day"), 2).alias("avg_event_time_day"),
        F.sum("event_time_day").alias("total_event_time_day"),
    )
)

result = web_events_stats.filter(F.col("avg_event_time_day") < 33)
