from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy(F.month("event_time").alias("event_month"))
    .agg(F.countDistinct("duration_sec").alias("unique_event_types"))
)
