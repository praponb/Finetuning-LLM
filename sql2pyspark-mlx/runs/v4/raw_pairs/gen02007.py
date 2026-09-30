from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("event_time") >= "2024-03-02")
    .select("id", "event_type", "event_time")
    .orderBy(F.col("id").desc(), "event_time")
    .limit(20)
)
