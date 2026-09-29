from pyspark.sql import functions as F

result = (
    df_web_events
    .select("id", "page", "event_time", "duration_sec")
    .orderBy(F.col("event_time").desc(), "page")
)
