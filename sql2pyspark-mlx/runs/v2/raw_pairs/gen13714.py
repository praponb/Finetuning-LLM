from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("page") == "/product")
    .groupBy(F.quarter("event_time").alias("event_quarter"), "event_type")
    .agg(F.countDistinct("duration_sec").alias("unique_sessions"))
)
