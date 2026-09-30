from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy("event_type", F.date_trunc("MONTH", "event_time").alias("event_month"))
    .agg(F.count("duration_sec").alias("duration_sec_count"))
    .filter(F.col("duration_sec_count") >= 5)
    .orderBy("event_month", "event_type")
    .limit(50)
)
