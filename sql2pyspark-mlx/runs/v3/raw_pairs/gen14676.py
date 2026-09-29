from pyspark.sql import functions as F

result = (
    df_web_events
    .select("id", "event_time")
    .orderBy("event_time", F.col("id").desc())
)
