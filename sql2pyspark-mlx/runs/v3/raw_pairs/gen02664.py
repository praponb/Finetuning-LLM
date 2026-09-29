from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("event_time") >= "2024-11-20")
    .groupBy("event_type")
    .agg(F.count("*").alias("cnt"))
    .filter(F.col("cnt") > 9)
    .select("event_type", "cnt")
    .orderBy(F.col("cnt").desc())
)
