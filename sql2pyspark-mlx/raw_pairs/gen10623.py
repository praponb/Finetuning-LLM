from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy("event_type", "page")
    .agg((F.max("duration_sec") - F.min("duration_sec")).alias("duration_sec_range"))
)
