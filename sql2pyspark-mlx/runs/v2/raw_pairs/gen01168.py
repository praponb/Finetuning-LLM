from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("session_id") == 4)
    .groupBy(F.month("event_time").alias("event_month"))
    .agg(F.count("duration_sec").alias("duration_sec_count"))
)
