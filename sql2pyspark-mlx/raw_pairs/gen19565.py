from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.to_date("event_time") >= "2024-09-01")
    .groupBy("event_type")
    .agg(F.count("*").alias("cnt"))
    .filter(F.col("cnt") >= 3)
    .orderBy("event_type")
)
