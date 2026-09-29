from pyspark.sql import functions as F

max_duration_sec = df_web_events.filter(F.col("event_time") >= "2023-12-15").agg(F.max("duration_sec")).first()[0]

result = (
    df_web_events
    .filter(F.col("duration_sec") > max_duration_sec)
    .select("id", "duration_sec", "page", "event_time")
    .orderBy(F.col("duration_sec").desc(), "id")
    .limit(3)
)
