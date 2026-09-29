from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy("event_type")
    .agg(F.min("duration_sec").alias("min_duration_sec"))
    .filter(F.col("min_duration_sec") < 260)
    .orderBy("event_type")
)
