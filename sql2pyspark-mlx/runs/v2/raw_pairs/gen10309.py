from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.hour("event_time").between(7, 15))
    .groupBy("event_type")
    .agg(F.round(F.avg("duration_sec"), 2).alias("avg_duration_sec"))
)
