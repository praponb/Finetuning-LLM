from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy(F.year("event_time").alias("event_year"))
    .agg(F.sum("duration_sec").alias("total_duration_sec"))
    .orderBy("event_year")
    .limit(5)
)
