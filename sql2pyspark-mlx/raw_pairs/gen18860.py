from pyspark.sql import functions as F

result = (
    df_web_events
    .cube("event_type", "page")
    .agg(F.count("duration_sec").alias("duration_sec_count"))
    .orderBy("duration_sec_count")
)
