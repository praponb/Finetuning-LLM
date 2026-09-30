from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.to_date("event_time") < "2023-10-01")
    .agg(F.min("event_time").alias("first_event_time"))
    .orderBy("first_event_time")
)
