from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("session_id").isin([15, 27, 28]))
    .groupBy("event_type")
    .agg((F.max("duration_sec") - F.min("duration_sec")).alias("duration_sec_range"))
    .filter(F.col("duration_sec_range") > 21)
    .select("event_type", "duration_sec_range")
)
