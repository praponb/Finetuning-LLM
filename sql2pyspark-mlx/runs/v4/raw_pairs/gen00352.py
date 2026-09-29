from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy(F.to_date("event_time").alias("event_day"))
    .agg(F.min("event_time").alias("min_event_time"))
    .orderBy("event_day")
    .limit(5)
)
