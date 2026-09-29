from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy(F.date_format("event_time", "yyyy-MM").alias("event_month"), "event_type")
    .agg((F.max("duration_sec") - F.min("duration_sec")).alias("duration_sec_range"))
    .filter(F.col("duration_sec_range") < 71)
)
