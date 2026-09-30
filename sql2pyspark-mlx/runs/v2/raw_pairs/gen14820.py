from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy("page", F.month("event_time").alias("event_month"))
    .agg(F.min("event_time").alias("first_event_time"))
)
