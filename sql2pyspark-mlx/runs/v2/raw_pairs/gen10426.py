from pyspark.sql import functions as F

recent_web_events = df_web_events.filter(F.col("event_type") == "add_to_cart")

summary = (
    recent_web_events
    .groupBy("page", F.date_trunc("MONTH", "event_time").alias("event_month"))
    .agg(
        F.min("event_time").alias("first_event_time"),
        F.avg("duration_sec").alias("avg_duration_sec"),
    )
)

result = (
    summary
    .filter(F.col("avg_duration_sec") < 290)
    .orderBy(F.col("avg_duration_sec").desc(), "page", "event_month")
    .limit(10)
)
