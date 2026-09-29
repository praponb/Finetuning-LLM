from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("session_id") == 38)
    .groupBy("page", "event_type")
    .agg((F.max("duration_sec") - F.min("duration_sec")).alias("duration_sec_range"))
)
