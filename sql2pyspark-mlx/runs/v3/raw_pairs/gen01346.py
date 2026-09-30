from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("session_id") == 3)
    .groupBy("event_type")
    .agg(F.count("duration_sec").alias("duration_sec_count"))
    .filter(F.col("duration_sec_count") >= 6)
)
