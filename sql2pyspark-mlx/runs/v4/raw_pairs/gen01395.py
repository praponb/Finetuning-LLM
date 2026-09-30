from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy("event_type", F.year("event_time").alias("event_year"))
    .agg((F.max("duration_sec") - F.min("duration_sec")).alias("duration_sec_range"))
    .orderBy(F.col("event_year").desc(), "event_type")
    .limit(5)
)
