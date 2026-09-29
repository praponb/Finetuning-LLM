from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("event_type") == "click")
    .groupBy(F.hour("event_time").alias("event_hour"))
    .agg(F.max("duration_sec").alias("max_duration_sec"))
    .filter(F.col("max_duration_sec") > 310)
    .orderBy(F.col("max_duration_sec").desc())
)
