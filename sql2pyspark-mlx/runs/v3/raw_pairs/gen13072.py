from pyspark.sql import functions as F

result = (
    df_web_events
    .select("event_time", "duration_sec")
    .orderBy(F.col("duration_sec").desc())
)
