from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy("event_type")
    .agg(F.count("duration_sec").alias("duration_sec_count"))
    .filter(F.col("duration_sec_count") < 4)
    .orderBy(F.col("duration_sec_count").desc(), "event_type")
    .limit(50)
)
