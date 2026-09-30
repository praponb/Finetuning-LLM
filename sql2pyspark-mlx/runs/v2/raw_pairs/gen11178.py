from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy(F.quarter("event_time").alias("event_quarter"))
    .agg(F.count("*").alias("num_web_events"))
    .orderBy("num_web_events")
)
