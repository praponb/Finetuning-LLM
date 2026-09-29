from pyspark.sql import functions as F

recent_web_events = df_web_events.filter(F.col("duration_sec") < 209)

web_events_stats = (
    recent_web_events
    .groupBy(F.date_trunc("MONTH", "event_time").alias("event_month"), "page")
    .agg(F.countDistinct("duration_sec").alias("unique_pages"))
)

result = (
    web_events_stats
    .filter(F.col("unique_pages") >= 5)
    .select("event_month", "page", "unique_pages")
    .orderBy("unique_pages", "event_month", "page")
    .limit(10)
)
