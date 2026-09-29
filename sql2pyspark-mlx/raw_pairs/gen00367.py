from pyspark.sql import functions as F

result = (
    df_web_events
    .select("id", "event_type", "page", "event_time")
    .orderBy("page", F.col("id").desc())
)
