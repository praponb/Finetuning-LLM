from pyspark.sql import functions as F

src = df_web_events.filter(F.col("page") == "/checkout")

web_events_stats = (
    src
    .groupBy(F.month("event_time").alias("event_month"))
    .agg(
        F.avg("duration_sec").alias("avg_duration_sec"),
        F.sum("duration_sec").alias("total_duration_sec"),
    )
)

result = (
    web_events_stats
    .filter(F.col("avg_duration_sec") < 260)
    .select("event_month", "avg_duration_sec", "total_duration_sec")
)
