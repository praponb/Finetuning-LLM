from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("duration_sec").isNotNull() & F.hour("event_time").between(8, 13))
    .select("event_time", "duration_sec")
    .orderBy(F.col("duration_sec").desc(), F.col("event_time").desc())
)
